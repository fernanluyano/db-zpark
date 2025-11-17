package dev.fb.dbzpark
package subtask

import zio.{Executor, ZIO}

class SequentialRunner private (
  override val subtasks: Seq[WorkflowSubtask]
) extends SubtasksRunner {

  /**
   * Executes all subtasks in the configured sequence. The default implementation runs tasks sequentially.
   *
   * @return
   * A ZIO effect that completes when all subtasks have been processed
   */
  override def run(executor: Executor): ZIO[TaskEnvironment, Throwable, Unit] =
    ZIO.foreachDiscard(subtasks)(runOne).onExecutor(executor)
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
