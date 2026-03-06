package dev.fb.dbzpark
package subtask

import org.apache.spark.sql.Dataset
import zio.{Task, ZIO}

/**
 * A composable unit of work within a workflow that processes data through defined pipeline stages.
 *
 * Execution follows this sequence:
 *   1. [[preProcess]] — optional setup before the pipeline runs
 *   2. [[readSource]] → [[transformer]] → [[sink]] — the core pipeline
 *   3. [[postProcess]] — optional work after the pipeline completes
 *
 * The pipeline and post-processing behaviour on failure is controlled by [[ensurePostProcess]]:
 *   - `false` (default): [[postProcess]] runs only if the pipeline succeeds; pipeline errors propagate immediately.
 *   - `true`: [[postProcess]] always runs regardless of pipeline outcome; the original error is re-raised afterwards.
 *
 * All stages are wrapped with logging and timing via [[run]], which is `final` and cannot be overridden.
 * Implementations customise behaviour exclusively through the protected hook methods.
 */
trait WorkflowSubtask {
  val taskId: String
  val localPriority: Int         = 1
  val ensurePostProcess: Boolean = false

  /**
   * Executes the subtask with logging and timing.
   * @return
   *   A ZIO effect that runs the subtask in a TaskEnvironment
   */
  final def run: ZIO[TaskEnvironment, Throwable, Unit] =
    for {
      _   <- ZIO.logInfo(s"starting subtask $taskId")
      env <- ZIO.service[TaskEnvironment]
      _   <- ZIO.logSpan(s"subtask-$taskId")(runSubtask(env))
    } yield ()

  private def runSubtask(env: TaskEnvironment): Task[Unit] =
    for {
      _ <- preProcess(env)
      _ <- ZIO.logInfo("finished pre-processing")
      _ <- runPipeline(env)
      _ <- ZIO.logInfo(s"finished subtask $taskId")
    } yield ()

  private def runPipeline(env: TaskEnvironment): Task[Unit] = {
    val res = for {
      source      <- readSource(env)
      transformed <- transformer(env, source)
      _           <- sink(env, transformed)
      _           <- ZIO.logInfo("finished sink")
    } yield ()

    if (ensurePostProcess)
      res.foldZIO(
        failure = e => postProcess(env) *> ZIO.fail(e),
        success = _ => postProcess(env)
      )
    else
      res *> postProcess(env)
  }

  /**
   * Optional pre-processing step executed before reading data.
   *
   * Override this method to perform setup tasks such as creating directories, validating preconditions, or preparing
   * resources. The default implementation does nothing.
   *
   * @param env
   *   The task environment
   */
  protected def preProcess(env: TaskEnvironment): Task[Unit] = ZIO.unit

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
  protected def readSource(env: TaskEnvironment): Task[Dataset[_]]

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
  protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]]

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
  protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit]

  /**
   * Optional step executed after the pipeline completes. Can be used for any post-work the implementation requires,
   * such as cleanup, notifications, or metadata updates.
   *
   * Whether this runs on pipeline failure is governed by [[ensurePostProcess]]. The default implementation does nothing.
   *
   * @param env
   *   The task environment
   */
  protected def postProcess(env: TaskEnvironment): Task[Unit] = ZIO.unit

  override def equals(obj: Any): Boolean = obj match {
    case that: WorkflowSubtask => this.taskId == that.taskId
    case _                     => false
  }

  override def hashCode(): Int = taskId.hashCode

  override def toString: String = s"{ taskId: $taskId, localPriority: $localPriority }"
}
