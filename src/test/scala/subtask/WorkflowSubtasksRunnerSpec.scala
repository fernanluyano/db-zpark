package dev.fb.dbzpark
package subtask

import org.apache.spark.sql.{Dataset, SparkSession}
import zio._
import zio.test._

object WorkflowSubtasksRunnerSpec extends ZIOSpecDefault {
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
    override val context = SubtaskContext(name)

    override def readSource(env: TaskEnvironment): Dataset[_] =
      throw new RuntimeException(s"Simulated failure in $name")

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] = inDs

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit = {}
  }

  override def spec = suite("WorkflowSubtasksRunner")(
    test("SequentialRunner should execute all tasks successfully") {
      // Create successful subtasks
      val task1 = new SuccessfulSubtask("task1")
      val task2 = new SuccessfulSubtask("task2")
      val task3 = new SuccessfulSubtask("task3")

      // Run the sequential runner
      val runner = Factory.SequentialRunner(Seq(task1, task2, task3))
      runner.run.provide(taskEnvLayer).as(assertTrue(true))
    },
    test("SequentialRunner should fail when a task fails") {
      // Mix successful and failing subtasks
      val task1 = new SuccessfulSubtask("task1")
      val task2 = new FailingSubtask("task2")
      val task3 = new SuccessfulSubtask("task3")

      // Run the sequential runner
      val runner = Factory.SequentialRunner(Seq(task1, task2, task3))
      runner.run.provide(taskEnvLayer).exit.map { exit =>
        // The runner should fail with an exception from task2
        assertTrue(
          exit.isFailure,
          exit.causeOption
            .flatMap(cause => cause.failureOption)
            .exists(_.getMessage.contains("Simulated failure in task2"))
        )
      }
    },
    test("ParallelRunner should execute all tasks successfully") {
      // Create several successful subtasks
      val tasks = (1 to 5).map(i => new SuccessfulSubtask(s"task$i")).toSeq

      // Run the parallel runner
      val runner = Factory.ParallelRunner(tasks, 3) // Use 3 as concurrency
      runner.run.provide(taskEnvLayer).as(assertTrue(true))
    },
    test("ParallelRunner should fail when a task fails") {
      // Mix successful and failing subtasks
      val tasks = Seq(
        new SuccessfulSubtask("task1"),
        new SuccessfulSubtask("task2"),
        new FailingSubtask("task3"),
        new SuccessfulSubtask("task4"),
        new SuccessfulSubtask("task5")
      )

      // Run the parallel runner
      val runner = Factory.ParallelRunner(tasks, 3)
      runner.run.provide(taskEnvLayer).exit.map { exit =>
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
