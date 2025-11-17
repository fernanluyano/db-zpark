package dev.fb.dbzpark
package subtask

import org.apache.spark.sql.SparkSession

/**
 * A simple example demonstrating a basic workflow with a single subtask.
 *
 * This object extends WorkflowTask, which provides the framework for running
 * Spark jobs in Databricks.
 */
object SimpleApp extends WorkflowTask {

  /**
   * Builds the task environment that provides the SparkSession and configuration
   * needed for all subtasks in this workflow. Extended if you need to bootstrap more dependencies
   *
   * @return A TaskEnvironment containing the Spark session and application name
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
   * Defines how subtasks should be executed in this workflow.
   *
   * This example uses a singleton subtask execution model, meaning there is only
   * one subtask that runs on a single-threaded executor.
   *
   * @return An ExecutionModel configured with the subtask(s) for this workflow
   */
  override protected def getExecutionModel: ExecutionModel = {
    val subtask = new SimpleSubtask(
      ignoreAndLogFailures = false, // Log failures and stop execution
      name = "MySubtask"
    )
    ExecutionModel.singletonSubtask(subtask)
  }
}
