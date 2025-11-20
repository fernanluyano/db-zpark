package dev.fb.dbzpark
package subtask

import org.apache.spark.sql.{Dataset, SparkSession}
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
    override def sparkSession: SparkSession = spark
    override def appName: String            = "MyAppName"
  }

  // Simple case class for test data
  case class TestData(id: Int, value: String)

  // Track execution order for verification
  val executionOrder = new ListBuffer[String]()

  // Create a concrete implementation of WorkflowSubtask for testing
  class TestSubtask extends WorkflowSubtask {
    override protected val ignoreAndLogFailures: Boolean = false
    override def getContext                              = SimpleContext("test-subtask")

    override def preProcess(env: TaskEnvironment): Unit =
      executionOrder += "preProcess"
    // Simulate some pre-processing setup

    override def readSource(env: TaskEnvironment): Dataset[_] = {
      executionOrder += "readSource"
      // Create a simple test dataset
      import spark.implicits._
      Seq(
        TestData(1, "value1"),
        TestData(2, "value2"),
        TestData(3, "value3")
      ).toDS()
    }

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] = {
      executionOrder += "transformer"
      // Apply a simple transformation - filter records with id > 1
      import spark.implicits._
      inDs.as[TestData].filter(_.id > 1)
    }

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit = {
      executionOrder += "sink"
      // In a real implementation, this would write to a destination
      // For testing, just materialize the dataset and check count
      val count = outDs.count()
      if (count != 2) {
        throw new AssertionError(s"Expected 2 records after transformation, but got $count")
      }
    }

    override def postProcess(env: TaskEnvironment): Unit =
      executionOrder += "postProcess"
    // Simulate some cleanup or finalization
  }

  // Create a layer providing ZIOAppArgs
  val appArgsLayer: ZLayer[Any, Nothing, ZIOAppArgs] = ZIOAppArgs.empty

  // Create a test logger layer
  val testLoggingLayer: ZLayer[Any, Nothing, Unit] = ZLayer.succeed(())

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
        override protected val ignoreAndLogFailures: Boolean = false
        override def getContext                              = SimpleContext("failing-subtask")

        override def readSource(env: TaskEnvironment): Dataset[_] =
          throw new RuntimeException("Simulated read failure")

        override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] =
          inDs // Never called due to readSource failure

        override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit = {
          // Never called due to readSource failure
        }
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

        // Verify failure was logged
        messages.exists(_.contains("starting subtask failing-subtask")),

        // Verify subtask completion message was not logged
        !messages.exists(_.contains("finished subtask failing-subtask"))
      )
    },
    test("subtask ignores failures") {
      // Create a failing subtask
      val failingSubtask = new WorkflowSubtask {
        override protected val ignoreAndLogFailures: Boolean = true
        override def getContext                              = SimpleContext("failing-subtask")

        override def readSource(env: TaskEnvironment): Dataset[_] =
          throw new RuntimeException("Simulated read failure")

        override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] =
          inDs // Never called due to readSource failure

        override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit = {
          // Never called due to readSource failure
        }
      }

      val env = new TestTaskEnvironment()

      for {
        // Run the failing subtask and capture the exit
        _ <- failingSubtask.run.provide(ZLayer.succeed(env))

        // Check logs for failure indications
        logOutput <- ZTestLogger.logOutput
        messages   = logOutput.map(_.message())
      } yield assertTrue(
        // Verify failure was logged
        messages.exists(_.contains("starting subtask failing-subtask")),
        // Verify subtask failure message was logged
        messages.exists(_.contains(s"Subtask ${failingSubtask.getContext.name} failed: Simulated read failure"))
      )
    },
    test("subtask with default implementations for optional methods") {
      // Create a minimal subtask that only implements required methods
      val minimalSubtask = new WorkflowSubtask {
        override protected val ignoreAndLogFailures: Boolean = false
        override def getContext                              = SimpleContext("minimal-subtask")

        override def readSource(env: TaskEnvironment): Dataset[_] = {
          import spark.implicits._
          Seq(TestData(1, "minimal")).toDS()
        }

        override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] =
          inDs // Pass-through

        override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit =
          // Just count to materialize
          outDs.count()
      }

      val env = new TestTaskEnvironment()

      for {
        // Run the minimal subtask
        result <- minimalSubtask.run.provide(ZLayer.succeed(env))

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
