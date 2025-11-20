package dev.fb.dbzpark
package subtask

import zio.{Executor, Task, ZIO}

/**
 * Base trait for executing a sequence of workflow subtasks.
 *
 * Provides foundational execution behavior with error logging and precondition validation. Implementations define
 * specific execution strategies (sequential, concurrent, etc.).
 */
trait SubtasksRunner {

  /**
   * The collection of subtasks to be executed.
   */
  val subtasks: Seq[WorkflowSubtask]

  /**
   * Executes all subtasks according to the implementation's strategy.
   *
   * Implementations determine whether subtasks run sequentially, concurrently, or with custom orchestration logic.
   *
   * @param executor
   *   Optional executor for running tasks on a specific thread pool. If None, uses the default ZIO executor.
   * @return
   *   A ZIO effect that completes when all subtasks have been processed
   */
  def run(executor: Option[Executor]): ZIO[TaskEnvironment, Throwable, Unit]

  /**
   * Validates that all subtask names are unique.
   *
   * This precondition check ensures that subtasks can be uniquely identified in logs and metrics. Fails with
   * IllegalStateException if duplicate names are found.
   *
   * @return
   *   A Task that succeeds if all names are unique, fails otherwise
   */
  protected def checkUniqueNames: Task[Unit] =
    ZIO.attempt {
      subtasks
        .groupBy(_.getContext.name)
        .filter(_._2.size > 1)
    }.flatMap { repeated =>
      if (repeated.isEmpty) ZIO.succeed()
      else ZIO.fail(new IllegalStateException(s"subtask names must be unique: \n$repeated"))
    }
}
