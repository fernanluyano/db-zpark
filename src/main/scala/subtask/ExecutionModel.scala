package dev.fb.dbzpark
package subtask

import subtask.ConcurrentRunner.Strategy

import zio.{Executor, ZIO}

import java.util.concurrent.Executors

/**
 * Encapsulates a subtask runner and optional executor for workflow execution.
 *
 * ExecutionModel provides a unified interface for running workflows regardless of the underlying execution strategy
 * (sequential or concurrent). It pairs a SubtasksRunner with an optional Executor to control where tasks run.
 *
 * @param runner
 *   The subtasks runner that defines execution strategy
 * @param executor
 *   Optional executor for controlling thread pool (None uses default ZIO executor)
 */
class ExecutionModel private (val runner: SubtasksRunner, val executor: Option[Executor]) {

  /**
   * Executes the workflow using the configured runner and executor.
   *
   * @return
   *   A ZIO effect that completes when all subtasks have been processed
   */
  def run: ZIO[TaskEnvironment, Throwable, Unit] = runner.run(executor)
}

object ExecutionModel {

  /**
   * Default executor with thread pool size from NUM_THREADS environment variable (default: 4).
   *
   * This executor is lazily initialized and reused across concurrent execution models unless a custom executor is
   * provided.
   */
  private lazy val defaultExecutor = Executor.fromJavaExecutor(
    Executors.newFixedThreadPool(sys.env.getOrElse("NUM_THREADS", "4").toInt)
  )

  /**
   * Creates an execution model for a single subtask.
   *
   * The subtask runs sequentially (since there's only one). This is a convenience method equivalent to
   * sequential(Seq(subtask)).
   *
   * @param subtask
   *   The subtask to execute
   * @return
   *   An ExecutionModel configured for single subtask execution
   */
  def singleton(subtask: WorkflowSubtask): ExecutionModel =
    sequential(Seq(subtask))

  /**
   * Creates an execution model that runs subtasks sequentially.
   *
   * Subtasks execute one after another in the order provided. Use this when tasks have dependencies or to limit
   * resource usage.
   *
   * @param subtasks
   *   The subtasks to execute in sequence
   * @return
   *   An ExecutionModel configured for sequential execution
   */
  def sequential(subtasks: Seq[WorkflowSubtask]): ExecutionModel = {
    val runner = SequentialRunner(subtasks)
    new ExecutionModel(runner, None)
  }

  /**
   * Creates an execution model that runs subtasks concurrently.
   *
   * Supports two strategies:
   *   - NO_DEPENDENCIES: All tasks run concurrently
   *   - GROUP_DEPENDENCIES: Groups run sequentially, tasks within groups run concurrently
   * @param subtasks
   *  The subtasks to execute
   * @param strategy
   *  Execution strategy (NO_DEPENDENCIES or GROUP_DEPENDENCIES)
   * @param executor
   *  Executor for controlling thread pool (default: uses NUM_THREADS environment variable)
   * @param maxRunningTasks
   *  Maximum number of subtasks to run concurrently (default: 4)
   * @return
   *   An ExecutionModel configured for concurrent execution
   */
  def concurrent(
    subtasks: Seq[WorkflowSubtask],
    strategy: Strategy,
    executor: Executor = defaultExecutor,
    maxRunningTasks: Int = 4
  ): ExecutionModel = {
    val runner = ConcurrentRunner(subtasks, strategy, maxRunningTasks)
    new ExecutionModel(runner, Some(executor))
  }
}
