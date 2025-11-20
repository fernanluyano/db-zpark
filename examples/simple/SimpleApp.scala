import subtask.ExecutionModel

import org.apache.spark.sql.SparkSession

/**
 * Simple example application demonstrating the subtask workflow framework.
 *
 * This app creates a single subtask that reads from a bronze table, adds a timestamp, and writes to a silver table.
 * The framework handles execution, logging, and error handling.
 */
object SimpleApp extends WorkflowTask {

  /**
   * Builds the task environment containing the Spark session and application configuration.
   */
  override protected def buildTaskEnvironment: TaskEnvironment = new TaskEnvironment {
    override def sparkSession: SparkSession = SparkSession
      .builder()
      .appName(appName)
      .master("local[2]") // Run locally with 2 cores
      .getOrCreate()

    override def appName: String = "MyApp"
  }

  /**
   * Defines the execution model for this workflow.
   *
   * Uses ExecutionModel.singleton() for a single subtask. For multiple subtasks, use:
   * - ExecutionModel.sequential() - runs subtasks one after another
   * - ExecutionModel.concurrent() - runs subtasks in parallel with configurable strategies
   */
  override protected def getExecutionModel: ExecutionModel = {
    val subtask = new SimpleSubtask(
      ignoreAndLogFailures = false,
      name = "MySubtask",
      sourceTable = SfAccountBronze,
      targetTable = SfAccountSilver
    )
    ExecutionModel.singleton(subtask)
  }
}
