package dev.fb.dbzpark
package subtask

import org.apache.spark.sql.Dataset
import zio.{Task, ZIO}

/**
 * A composable unit of work within a workflow that processes data through defined pipeline stages.
 *
 * This trait implements a standardized sequence of operations:
 * <ul>
 *   <li>Pre-processing setup</li>
 *   <li>Data source reading</li>
 *   <li>Data transformation</li>
 *   <li>Writing to a sink</li>
 *   <li>Post-processing and / or cleanup</li>
 * </ul>
 * Each stage is tracked with appropriate logging for monitoring and diagnostics.
 */
trait WorkflowSubtask {

  protected val ignoreAndLogFailures: Boolean

  /**
   * Executes the subtask with logging and timing.
   *
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
   * @param env
   *   The task environment containing Spark session, app configuration and other dependencies needed.
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
   */
  def getContext: SubtaskContext

  /**
   * Optional pre-processing step executed before reading data.
   *
   * @param env
   *   The task environment
   */
  protected def preProcess(env: TaskEnvironment): Unit = ()

  /**
   * Reads data from a source.
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
   * @param env
   *   The task environment
   * @param outDs
   *   The dataset to write
   */
  protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit

  /**
   * Optional post-processing step executed after all other steps complete.
   *
   * @param env
   *   The task environment
   */
  protected def postProcess(env: TaskEnvironment): Unit = ()
}
