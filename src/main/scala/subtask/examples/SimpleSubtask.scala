package dev.fb.dbzpark
package subtask.examples

import subtask.{SimpleContext, WorkflowSubtask}

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.current_timestamp

/**
 * A simple example subtask that demonstrates the basic structure of a WorkflowSubtask.
 *
 * This subtask implements the standard ETL pattern:
 * 1. Pre-process (optional setup)
 * 2. Read source data
 * 3. Transform the data
 * 4. Write to sink
 * 5. Post-process (optional cleanup)
 *
 * @param ignoreAndLogFailures If true, failures are logged but don't stop execution.
 *                             If false, failures will cause the workflow to fail.
 * @param name The name of this subtask, used in logging and tracking
 */
private class SimpleSubtask(
  override protected val ignoreAndLogFailures: Boolean,
  val name: String
) extends WorkflowSubtask {

  /** Context metadata for this subtask */
  override def getContext: SimpleContext = SimpleContext(name)

  /**
   * Optional pre-processing step executed before reading data.
   *
   * Use this for:
   * - Setting up temporary directories
   * - Validating prerequisites
   * - Initializing connections
   *
   * @param env The task environment containing SparkSession and configuration
   */
  override protected def preProcess(env: TaskEnvironment): Unit =
    // Implementation is optional - override only if needed
    println("optional")

  /**
   * Optional post-processing step executed after all other steps complete.
   *
   * Use this for:
   * - Cleanup operations (DO NOT STOP THE SPARK SESSION)
   * - Updating metadata tables
   * - Sending notifications
   *
   * @param env The task environment containing SparkSession and configuration
   */
  override protected def postProcess(env: TaskEnvironment): Unit =
    // Implementation is optional - override only if needed
    println("optional")

  /**
   * Reads data from the source.
   *
   * This method defines where your input data comes from.
   * It can read from various sources: Delta tables, Parquet files, CSV, etc.
   *
   * @param env The task environment containing SparkSession and configuration
   * @return A Dataset containing the source data
   */
  override protected def readSource(env: TaskEnvironment): Dataset[_] =
    env.sparkSession.read
      .format("delta")
      .table("catalog.schema.table")

  /**
   * Transforms the input dataset.
   *
   * This method applies business logic to transform the input data.
   * This is where you implement your data processing logic.
   *
   * @param env The task environment containing SparkSession and configuration
   * @param inDs The input dataset to transform
   * @return The transformed dataset
   */
  override protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] =
    inDs.withColumn("_ingestion_time", current_timestamp())

  /**
   * Writes the transformed data to the destination.
   *
   * This method defines where your output data goes.
   * It can write to Delta tables, Parquet files, or other formats.
   * In this example, it just displays the data.
   *
   * @param env The task environment containing SparkSession and configuration
   * @param outDs The dataset to write
   */
  override protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit =
    outDs.show(20, truncate = false)
}
