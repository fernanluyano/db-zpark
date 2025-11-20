package dev.fb.dbzpark
package subtask

import org.apache.spark.sql.Dataset
import zio.{Task, ZIO}

/**
 * A composable unit of work within a workflow that processes data through defined pipeline stages.
 *
 * This trait implements a standardized sequence of operations:
 *   - Pre-processing setup
 *   - Data source reading
 *   - Data transformation
 *   - Writing to a sink
 *   - Post-processing and/or cleanup
 *
 * Each stage is tracked with appropriate logging for monitoring and diagnostics. Implementations can override any
 * stage to customize behavior, and can implement arbitrary logic - not limited to data processing.
 */
trait WorkflowSubtask {

  /**
   * Controls failure handling behavior. If true, failures are logged but do not fail the workflow.
   */
  protected val ignoreAndLogFailures: Boolean

  /**
   * Executes the subtask with logging and timing.
   * @return
   *   A ZIO effect that runs the subtask in a TaskEnvironment
   */
  def run: ZIO[TaskEnvironment, Throwable, Unit] =
    for {
      _   <- ZIO.logInfo(s"starting subtask ${getContext.name}")
      env <- ZIO.service[TaskEnvironment]
      _   <- ZIO.logSpan(s"subtask-${getContext.name}")(runSubtask(env))
    } yield ()

  /**
   * Executes all stages of the subtask in sequence.
   *
   * Failures are handled according to the ignoreAndLogFailures flag. If true, errors are logged but the effect
   * succeeds. If false, errors propagate to the caller.
   *
   * @param env
   *   The task environment containing Spark session, app configuration and other dependencies needed
   * @return
   *   A Task representing the subtask execution
   */
  private def runSubtask(env: TaskEnvironment): Task[Unit] = {
    val flow = for {
      _           <- ZIO.attempt(preProcess(env))
      _           <- ZIO.logInfo("finished pre-processing")
      source      <- ZIO.attempt(readSource(env))
      transformed <- ZIO.attempt(transformer(env, source))
      _           <- ZIO.attempt(sink(env, transformed))
      _           <- ZIO.logInfo("finished sink")
      _           <- ZIO.attempt(postProcess(env))
      _           <- ZIO.logInfo(s"finished subtask ${getContext.name}")
    } yield ()

    flow.foldZIO(
      success = _ => ZIO.unit,
      failure = e =>
        if (ignoreAndLogFailures)
          ZIO.logError(s"Subtask ${getContext.name} failed: ${e.getMessage}").unit
        else
          ZIO.fail(e)
    )
  }

  /**
   * Metadata about the subtask.
   *
   * @return
   *   SubtaskContext containing the subtask name and optional grouping information
   */
  def getContext: SubtaskContext

  /**
   * Optional pre-processing step executed before reading data.
   *
   * Override this method to perform setup tasks such as creating directories, validating preconditions, or preparing
   * resources. The default implementation does nothing.
   *
   * @param env
   *   The task environment
   */
  protected def preProcess(env: TaskEnvironment): Unit = ()

  /**
   * Reads data from a source.
   *
   * This method must be implemented to define where and how data is read. Common sources include Delta tables, Parquet
   * files, CSV files, or external APIs.
   *
   * @param env
   *   The task environment
   * @return
   *   A Dataset containing the source data
   */
  protected def readSource(env: TaskEnvironment): Dataset[_]

  /**
   * Transforms the input dataset.
   *
   * This method must be implemented to define the data transformation logic. Transformations can include filtering,
   * aggregations, joins, column additions, or any Spark DataFrame operation.
   *
   * @param env
   *   The task environment
   * @param inDs
   *   The input dataset to transform
   * @return
   *   A transformed dataset
   */
  protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_]

  /**
   * Writes the transformed data to a destination.
   *
   * This method must be implemented to define where and how transformed data is written. Common destinations include
   * Delta tables, Parquet files, or external storage systems.
   *
   * @param env
   *   The task environment
   * @param outDs
   *   The dataset to write
   */
  protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit

  /**
   * Optional post-processing step executed after all other steps complete.
   *
   * Override this method to perform cleanup tasks such as deleting temporary files, sending notifications, or updating
   * metadata. The default implementation does nothing.
   *
   * @param env
   *   The task environment
   */
  protected def postProcess(env: TaskEnvironment): Unit = ()
}
