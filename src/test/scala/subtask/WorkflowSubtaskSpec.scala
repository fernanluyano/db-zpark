package dev.fb.dbzpark
package subtask

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import zio._
import zio.test._

import scala.collection.mutable.ListBuffer

object WorkflowSubtaskSpec extends ZIOSpecDefault {
  // Create a SparkSession for testing
  private val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("subtask-test")
    .getOrCreate()

  // Create a test implementation of TaskEnvironment
  class TestTaskEnvironment extends TaskEnvironment {
    override def sparkSession: SparkSession   = spark
    override def appName: String              = "MyAppName"
    override def subtasksGraph: SubtasksGraph = ???
    override def subtasksExecutor: Executor   = ???
    override def maxConcurrentSubtasks: Int   = 1
  }

  // Simple case class for test data
  case class TestData(id: Int, value: String)

  // Track execution order for verification
  val executionOrder = new ListBuffer[String]()

  // Create a concrete implementation of WorkflowSubtask for testing
  class TestSubtask extends WorkflowSubtask {
    override val taskId: String = "test-subtask"

    override def preProcess(env: TaskEnvironment): Task[Unit] = ZIO.attempt {
      executionOrder += "preProcess"
    }

    override def readSource(env: TaskEnvironment): Task[Dataset[_]] = ZIO.attempt {
      executionOrder += "readSource"
      // Create a simple test dataset
      import spark.implicits._
      Seq(
        TestData(1, "value1"),
        TestData(2, "value2"),
        TestData(3, "value3")
      ).toDS()
    }

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] = ZIO.attempt {
      executionOrder += "transformer"
      // Apply a simple transformation - filter records with id > 1
      import spark.implicits._
      inDs.as[TestData].filter(_.id > 1)
    }

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] = ZIO.attempt {
      executionOrder += "sink"
      // In a real implementation, this would write to a destination
      // For testing, just materialize the dataset and check count
      val count = outDs.count()
      if (count != 2) {
        throw new AssertionError(s"Expected 2 records after transformation, but got $count")
      }
    }

    override def postProcess(env: TaskEnvironment): Task[Unit] = ZIO.attempt {
      executionOrder += "postProcess"
    }
  }

  override def spec = suite("WorkflowSubtask")(
    test("subtask executes all stages in the correct order") {
      // Clear the execution tracker before test
      executionOrder.clear()

      val subtask = new TestSubtask()
      val env     = new TestTaskEnvironment()

      for {
        // Run the subtask
        _ <- subtask.run.provide(ZLayer.succeed(env))

        // Verify log outputs
        logOutput <- ZTestLogger.logOutput
        messages   = logOutput.map(_.message())

        // Check execution order
        executionSequence = executionOrder.toList
      } yield assertTrue(
        // Verify the subtask logged starting and completion
        messages.exists(_.contains("starting subtask test-subtask")),
        messages.exists(_.contains("finished subtask test-subtask")),

        // Verify each stage completion was logged
        messages.exists(_.contains("finished pre-processing")),
        messages.exists(_.contains("finished sink")),

        // Verify stages executed in correct order
        executionSequence == List("preProcess", "readSource", "transformer", "sink", "postProcess")
      )
    },
    test("subtask handles failures appropriately") {
      // Create a failing subtask
      val failingSubtask = new WorkflowSubtask {
        override val taskId: String = "failing-subtask"

        override def readSource(env: TaskEnvironment): Task[Dataset[_]] =
          ZIO.fail(new RuntimeException("Simulated read failure"))

        override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
          ZIO.succeed(inDs) // Never called due to readSource failure

        override def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] =
          ZIO.unit // Never called due to readSource failure
      }

      val env = new TestTaskEnvironment()

      for {
        // Run the failing subtask and capture the exit
        exit <- failingSubtask.run.provide(ZLayer.succeed(env)).exit

        // Check logs for failure indications
        logOutput <- ZTestLogger.logOutput
        messages   = logOutput.map(_.message())
      } yield assertTrue(
        // Verify task failed
        exit.isFailure,

        // Verify the subtask was dispatched
        messages.exists(_.contains("starting subtask failing-subtask")),

        // Verify the exit contains the error message
        exit.causeOption
          .flatMap(_.failureOption)
          .exists(_.getMessage.contains("Simulated read failure"))
      )
    },
    test("subtask with default implementations for optional methods") {
      executionOrder.clear()

      // Create a minimal subtask that only implements required methods
      val minimalSubtask = new WorkflowSubtask {
        override val taskId: String = "minimal-subtask"

        override def readSource(env: TaskEnvironment): Task[Dataset[_]] = ZIO.attempt {
          import spark.implicits._
          Seq(TestData(1, "minimal")).toDS()
        }

        override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
          ZIO.succeed(inDs) // Pass-through

        override def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] =
          ZIO.attempt(outDs.count()).unit // Just count to materialize
      }

      val env = new TestTaskEnvironment()

      for {
        // Run the minimal subtask
        _ <- minimalSubtask.run.provide(ZLayer.succeed(env))

        // Verify logs
        logOutput <- ZTestLogger.logOutput
        messages   = logOutput.map(_.message())
      } yield assertTrue(
        // Verify task succeeded without implementing optional methods
        messages.exists(_.contains("starting subtask minimal-subtask")),
        messages.exists(_.contains("finished subtask minimal-subtask"))
      )
    }
  ) @@ TestAspect.beforeAll(ZIO.attempt(executionOrder.clear()))
}
