package dev.fb.dbzpark
package subtask

import zio.ZIO

/**
 * Base trait for executing a sequence of workflow subtasks. Provides foundational execution behavior with error
 * logging.
 */
trait WorkflowSubtasksRunner {

  /** The collection of subtasks to be executed */
  val subtasks: Seq[WorkflowSubtask]

  /**
   * Executes all subtasks in the configured sequence. The default implementation runs tasks sequentially.
   *
   * @return
   *   A ZIO effect that completes when all subtasks have been processed
   */
  def run: ZIO[TaskEnvironment, Throwable, Unit] =
    ZIO.foreachDiscard(subtasks)(runOne)

  /**
   * Executes a single subtask with error logging.
   *
   * @param subtask
   *   The subtask to execute
   * @return
   *   A ZIO effect representing the execution of the subtask
   */
  protected def runOne(subtask: WorkflowSubtask): ZIO[TaskEnvironment, Throwable, Unit] =
    subtask.run.tapError(e => ZIO.logError(s"Subtask ${subtask.context.name} failed: ${e.getMessage}"))
}

/**
 * Trait that enables concurrent execution of subtasks with controlled parallelism. Can be mixed into a
 * WorkflowSubtasksRunner to enable parallel processing.
 */
trait ConcurrentRunner extends WorkflowSubtasksRunner {

  /** Defines the maximum number of subtasks to execute in parallel */
  val concurrency: Int

  /**
   * Overrides the default execution to run subtasks concurrently. Limits parallelism to the configured concurrency
   * level.
   *
   * @return
   *   A ZIO effect that completes when all subtasks have been processed
   */
  override def run: ZIO[TaskEnvironment, Throwable, Unit] =
    for {
      _ <- ZIO.foreachParDiscard(subtasks)(runOne).withParallelism(concurrency)
    } yield ()
}

/**
 * Factory object providing concrete implementations of workflow subtask runners.
 */
object Factory {

  /**
   * A runner that executes subtasks sequentially in the order provided. Fails fast if any subtask fails.
   */
  class SequentialRunner private (
    override val subtasks: Seq[WorkflowSubtask]
  ) extends WorkflowSubtasksRunner

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

  /**
   * A runner that executes subtasks concurrently with a specified level of parallelism. Fails if any subtask fails.
   */
  class ParallelRunner private (
    override val subtasks: Seq[WorkflowSubtask],
    override val concurrency: Int
  ) extends WorkflowSubtasksRunner
      with ConcurrentRunner

  object ParallelRunner {

    /**
     * Creates a new parallel runner.
     *
     * @param subtasks
     *   The subtasks to execute in parallel
     * @param concurrency
     *   The maximum number of subtasks to execute simultaneously
     * @return
     *   A new ParallelRunner instance
     */
    def apply(subtasks: Seq[WorkflowSubtask], concurrency: Int): ParallelRunner =
      new ParallelRunner(subtasks, concurrency)
  }
}
