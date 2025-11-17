package dev.fb.dbzpark
package subtask

import org.apache.spark.sql.{Dataset, SparkSession}
import zio._
import zio.test._

import java.util.concurrent.Executors

object ConcurrentRunnerSpec extends ZIOSpecDefault {
  // Create a SparkSession for testing
  private val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("runner-test")
    .getOrCreate()

  // Create a test environment
  class TestTaskEnvironment extends TaskEnvironment {
    override def sparkSession: SparkSession = spark
    override def appName: String            = "TestApp"
  }

  val taskEnvLayer: ZLayer[Any, Nothing, TaskEnvironment] = ZLayer.succeed(new TestTaskEnvironment)

  // Create a test executor
  private val testExecutor = Executor.fromJavaExecutor(Executors.newFixedThreadPool(3))

  // Create test subtasks
  class SuccessfulSubtask(val name: String) extends WorkflowSubtask {
    override protected val ignoreAndLogFailures: Boolean = false
    override val context = SubtaskContext(name)

    override def readSource(env: TaskEnvironment): Dataset[_] = {
      import spark.implicits._
      Seq((1, name)).toDF("id", "name")
    }

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] = inDs

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit =
      outDs.count() // Materialize the dataset
  }

  class FailingSubtask(val name: String) extends WorkflowSubtask {
    override protected val ignoreAndLogFailures: Boolean = false
    override val context = SubtaskContext(name)

    override def readSource(env: TaskEnvironment): Dataset[_] =
      throw new RuntimeException(s"Simulated failure in $name")

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] = inDs

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit = {}
  }

  override def spec = suite("ConcurrentRunner")(
    test("should execute all tasks successfully") {
      // Create several successful subtasks
      val tasks = (1 to 5).map(i => new SuccessfulSubtask(s"task$i")).toSeq

      // Run the parallel runner
      val runner = ConcurrentRunner(tasks)
      runner.run(testExecutor).provide(taskEnvLayer).as(assertTrue(true))
    },
    test("should fail when a task fails") {
      // Mix successful and failing subtasks
      val tasks = Seq(
        new SuccessfulSubtask("task1"),
        new SuccessfulSubtask("task2"),
        new FailingSubtask("task3"),
        new SuccessfulSubtask("task4"),
        new SuccessfulSubtask("task5")
      )

      // Run the parallel runner
      val runner = ConcurrentRunner(tasks)
      runner.run(testExecutor).provide(taskEnvLayer).exit.map { exit =>
        // The runner should fail with an exception
        assertTrue(
          exit.isFailure,
          exit.causeOption
            .flatMap(cause => cause.failureOption)
            .exists(_.getMessage.contains("Simulated failure in task3"))
        )
      }
    }
  )
}