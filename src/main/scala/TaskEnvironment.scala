package dev.fb.dbzpark

import dev.fb.dbzpark.subtask.SubtasksGraph
import org.apache.spark.sql.SparkSession
import zio.Executor

/**
 * Provides all runtime dependencies required to execute a subtask workflow.
 *
 * Implementations supply the Spark session, the task graph, the executor for parallel
 * dispatch, and the concurrency and failure policies consumed by [[subtask.SubtasksManager]].
 * Only [[failFast]] has a default; all other members must be implemented.
 */
trait TaskEnvironment {

  /**
   * The Spark session used by subtasks for data processing.
   * Implementations are responsible for building and configuring it.
   */
  def sparkSession: SparkSession

  /**
   * The Spark application name, used for identification in logs and the Spark UI.
   */
  def appName: String

  /**
   * The DAG of subtasks to execute. Consumed by [[subtask.SubtasksManager]] to determine
   * execution order and propagate failures to dependent nodes.
   */
  def subtasksGraph: SubtasksGraph

  /**
   * The ZIO executor on which subtasks are dispatched in parallel.
   * Typically backed by a fixed thread pool sized to match [[maxConcurrentSubtasks]].
   */
  def subtasksExecutor: Executor

  /**
   * The maximum number of subtasks that may run concurrently within a single batch.
   * [[subtask.SubtasksManager]] takes at most this many ready nodes per iteration.
   */
  def maxConcurrentSubtasks: Int

  /**
   * When true, the first subtask failure immediately halts the entire run and propagates
   * the error. When false (the default), failures are logged and the run continues,
   * skipping only the descendants of the failed node.
   */
  def failFast: Boolean = false
}
