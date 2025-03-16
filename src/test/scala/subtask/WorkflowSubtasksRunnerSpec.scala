package dev.fb.dbzpark
package subtask

import org.apache.spark.sql.{Dataset, SparkSession}
import zio._
import zio.test._
import dev.fb.dbzpark.subtask.Runners._

// Import the concrete runner implementations

object WorkflowSubtasksRunnerSpec extends ZIOSpecDefault {
  // Create a SparkSession for testing
  private val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("runner-test")
    .getOrCreate()

  // Create a test implementation of TaskEnvironment
  class TestTaskEnvironment extends TaskEnvironment {
    override def sparkSession: SparkSession = spark
    override def appName: String            = "RunnerTestApp"
  }

  // Simple case class for test data
  case class TestData(id: Int, value: String)

  // Create a test environment layer
  val taskEnvLayer: ZLayer[Any, Nothing, TaskEnvironment] = ZLayer.succeed(new TestTaskEnvironment)

  // Track execution for verification
  case class ExecutionState(started: Ref[List[String]],
                            completed: Ref[List[String]],
                            failed: Ref[List[String]])

  def makeExecutionState: UIO[ExecutionState] = for {
    started <- Ref.make(List.empty[String])
    completed <- Ref.make(List.empty[String])
    failed <- Ref.make(List.empty[String])
  } yield ExecutionState(started, completed, failed)

  // Factory for creating test subtasks
  def createSuccessfulSubtask(name: String, executionState: ExecutionState): WorkflowSubtask = new WorkflowSubtask {
    override val context = SubtaskContext(name, 1)

    override def readSource(env: TaskEnvironment): Dataset[_] = {
      executionState.started.update(list => name :: list).ignore
      // Create a simple test dataset
      import spark.implicits._
      Seq(TestData(1, s"value-$name")).toDS()
    }

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] = inDs

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit = {
      outDs.count() // Force evaluation
      executionState.completed.update(list => name :: list).ignore
    }
  }

  def createFailingSubtask(name: String, executionState: ExecutionState): WorkflowSubtask = new WorkflowSubtask {
    override val context = SubtaskContext(name, 1)

    override def readSource(env: TaskEnvironment): Dataset[_] = {
      executionState.started.update(list => name :: list).ignore
      executionState.failed.update(list => name :: list).ignore
      throw new RuntimeException(s"Simulated failure in $name")
    }

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] = inDs

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit = {
      // This should never be called
      executionState.completed.update(list => name :: list).ignore
    }
  }

  override def spec = {
    suite("WorkflowSubtasksRunner")(
      test("SequentialRunner should execute subtasks in order") {
        for {
          state <- makeExecutionState
          task1 = createSuccessfulSubtask("task1", state)
          task2 = createSuccessfulSubtask("task2", state)
          task3 = createSuccessfulSubtask("task3", state)

          runner = new SequentialRunner(Seq(task1, task2, task3))
          _ <- runner.run.provide(taskEnvLayer)

          started <- state.started.get
          completed <- state.completed.get
        } yield assertTrue(
          started == List("task3", "task2", "task1"),    // First in, last out (stack)
          completed == List("task3", "task2", "task1")   // Last completed is on top of the list
        )
      }
    )
  }
}