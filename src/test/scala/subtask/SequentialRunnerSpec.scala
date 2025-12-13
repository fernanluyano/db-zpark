package dev.fb.dbzpark
package subtask

import org.apache.spark.sql.{Dataset, SparkSession}
import zio._
import zio.test._

object SequentialRunnerSpec extends ZIOSpecDefault {
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

  // Create test subtasks
  class SuccessfulSubtask(val name: String) extends WorkflowSubtask {
    override protected val ignoreAndLogFailures: Boolean = false
    override def getContext                              = SimpleContext(name)

    override def readSource(env: TaskEnvironment): Task[Dataset[_]] = ZIO.attempt {
      import spark.implicits._
      Seq((1, name)).toDF("id", "name")
    }

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
      ZIO.succeed(inDs)

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] =
      ZIO.attempt(outDs.count()).unit // Materialize the dataset
  }

  class FailingSubtask(val name: String) extends WorkflowSubtask {
    override protected val ignoreAndLogFailures: Boolean = false
    override def getContext                              = SimpleContext(name)

    override def readSource(env: TaskEnvironment): Task[Dataset[_]] =
      ZIO.fail(new RuntimeException(s"Simulated failure in $name"))

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
      ZIO.succeed(inDs)

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] =
      ZIO.unit
  }

  override def spec = suite("SequentialRunner")(
    test("should execute all tasks successfully") {
      // Create successful subtasks
      val task1 = new SuccessfulSubtask("task1")
      val task2 = new SuccessfulSubtask("task2")
      val task3 = new SuccessfulSubtask("task3")

      // Run the sequential runner
      val runner = SequentialRunner(Seq(task1, task2, task3))

      for {
        _         <- runner.run(None).provide(taskEnvLayer)
        logOutput <- ZTestLogger.logOutput
        messages   = logOutput.map(_.message())
      } yield assertTrue(
        messages.exists(_.contains("task1")),
        messages.exists(_.contains("task2")),
        messages.exists(_.contains("task3"))
      )
    },
    test("should fail when a task fails") {
      // Mix successful and failing subtasks
      val task1 = new SuccessfulSubtask("task1")
      val task2 = new FailingSubtask("task2")
      val task3 = new SuccessfulSubtask("task3")

      // Run the sequential runner
      val runner = SequentialRunner(Seq(task1, task2, task3))

      for {
        exit      <- runner.run(None).provide(taskEnvLayer).exit
        logOutput <- ZTestLogger.logOutput
        messages   = logOutput.map(_.message())
      } yield
      // The runner should fail with an exception from task2
      assertTrue(
        exit.isFailure,
        exit.causeOption
          .flatMap(cause => cause.failureOption)
          .exists(_.getMessage.contains("Simulated failure in task2")),
        // task1 should have started
        messages.exists(_.contains("task1")),
        // task2 should have failed
        messages.exists(msg => msg.contains("task2") || msg.contains("Simulated failure"))
      )
    }
  )
}
