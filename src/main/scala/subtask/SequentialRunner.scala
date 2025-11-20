package dev.fb.dbzpark
package subtask

import zio.{Executor, ZIO}

/**
 * Executes subtasks sequentially, one after another.
 *
 * This runner processes subtasks in the order they appear in the sequence. Each subtask completes before the next one
 * begins. Use this when subtasks have dependencies or when you want to limit resource usage.
 *
 * @param subtasks
 *   The subtasks to execute in sequence
 */
class SequentialRunner private (
  override val subtasks: Seq[WorkflowSubtask]
) extends SubtasksRunner {

  /**
   * Executes all subtasks sequentially.
   *
   * Each subtask runs to completion before the next one starts. The executor parameter is ignored since sequential
   * execution doesn't require custom thread pool management.
   *
   * @param ignored
   *   Executor parameter (unused for sequential execution)
   * @return
   *   A ZIO effect that completes when all subtasks have been processed
   */
  override def run(ignored: Option[Executor] = None): ZIO[TaskEnvironment, Throwable, Unit] =
    ZIO.foreachDiscard(subtasks)(runOne)
}

object SequentialRunner {

  /**
   * Creates a new sequential runner.
   *
   * @param subtasks
   *   The subtasks to execute in sequence
   * @return
   *   A new SequentialRunner instance
   */
  def apply(subtasks: Seq[WorkflowSubtask]): SequentialRunner =
    new SequentialRunner(subtasks)
}
