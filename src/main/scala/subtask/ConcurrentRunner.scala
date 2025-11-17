package dev.fb.dbzpark
package subtask

import zio.{Executor, ZIO}

class ConcurrentRunner private (val subtasks: Seq[WorkflowSubtask]) extends SubtasksRunner {

  /**
   * Overrides the default execution to run subtasks concurrently. Limits parallelism to the configured concurrency
   * level.
   *
   * @return
   *   A ZIO effect that completes when all subtasks have been processed
   */
  override def run(executor: Executor): ZIO[TaskEnvironment, Throwable, Unit] =
    for {
      _ <- ZIO.foreachParDiscard(subtasks)(runOne).onExecutor(executor)
    } yield ()
}

object ConcurrentRunner {
  def apply(subtasks: Seq[WorkflowSubtask]): ConcurrentRunner =
    new ConcurrentRunner(subtasks)
}
