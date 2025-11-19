package dev.fb.dbzpark
package subtask

import zio.{Executor, Task, ZIO}

/**
 * Base trait for executing a sequence of workflow subtasks. Provides foundational execution behavior with error
 * logging.
 */
trait SubtasksRunner {

  /** The collection of subtasks to be executed */
  val subtasks: Seq[WorkflowSubtask]

  /**
   * Executes all subtasks in the configured sequence. The default implementation runs tasks sequentially.
   *
   * @return
   *   A ZIO effect that completes when all subtasks have been processed
   */
  def run(executor: Option[Executor]): ZIO[TaskEnvironment, Throwable, Unit]

  protected def preconditions: Task[Unit] =
    ZIO.attempt {
      subtasks
        .groupBy(_.getContext.name)
        .filter(_._2.size > 1)
    }.flatMap { repeated =>
      if (repeated.isEmpty) ZIO.succeed()
      else ZIO.fail(new IllegalStateException(s"subtask names must be unique: \n$repeated"))
    }

  /**
   * Executes a single subtask with error logging.
   *
   * @param subtask
   *   The subtask to execute
   * @return
   *   A ZIO effect representing the execution of the subtask
   */
  protected def runOne(subtask: WorkflowSubtask): ZIO[TaskEnvironment, Throwable, Unit] =
    subtask.run.tapError(e => ZIO.logError(s"Subtask ${subtask.getContext.name} failed: ${e.getMessage}"))
}
