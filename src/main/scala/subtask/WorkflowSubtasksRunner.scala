package dev.fb.dbzpark
package subtask

import zio.{Schedule, ZIO}

sealed trait WorkflowSubtasksRunner {
  val subtasks: Seq[WorkflowSubtask]

  def run: ZIO[TaskEnvironment, Throwable, Unit] =
    for {
      _ <- ZIO.foreachDiscard(subtasks)(runOne)
    } yield ()

  protected def runOne(subtask: WorkflowSubtask): ZIO[TaskEnvironment, Throwable, Unit] =
    subtask.run.tapError(e => ZIO.logError(s"Subtask ${subtask.context.name} failed: ${e.getMessage}"))

}

sealed trait ConcurrentRunner extends WorkflowSubtasksRunner {
  val concurrency: Int

  override def run: ZIO[TaskEnvironment, Throwable, Unit] =
    for {
      _ <- ZIO.foreachParDiscard(subtasks)(runOne).withParallelism(concurrency)
    } yield ()
}

sealed trait RetryableRunner extends WorkflowSubtasksRunner {
  val maxRetries: Int

  override protected def runOne(subtask: WorkflowSubtask): ZIO[TaskEnvironment, Throwable, Unit] =
    subtask.run
      .retry(Schedule.recurs(maxRetries))
      .tapError(e =>
        ZIO.logError(s"Subtask ${subtask.context.name} failed after $maxRetries attempts: ${e.getMessage}")
      )
}

sealed trait IgnoredFails extends WorkflowSubtasksRunner {
  override protected def runOne(subtask: WorkflowSubtask): ZIO[TaskEnvironment, Throwable, Unit] =
    super
      .runOne(subtask)
      .foldZIO(
        success = _ => ZIO.unit,
        failure = e => ZIO.logError(s"Subtask ${subtask.context.name} failed: ${e.getMessage}")
      )
}

object Runners {
  class SequentialRunner (
    override val subtasks: Seq[WorkflowSubtask]
  ) extends WorkflowSubtasksRunner

  class ParallelRunner (
    override val subtasks: Seq[WorkflowSubtask],
    override val concurrency: Int
  ) extends WorkflowSubtasksRunner
      with ConcurrentRunner

  class RetrySequentialRunner (
    override val subtasks: Seq[WorkflowSubtask],
    override val maxRetries: Int
  ) extends WorkflowSubtasksRunner
      with RetryableRunner

  class ResilientParallelRunner (
    override val subtasks: Seq[WorkflowSubtask],
    override val concurrency: Int,
    override val maxRetries: Int
  ) extends WorkflowSubtasksRunner
      with ConcurrentRunner
      with RetryableRunner
      with IgnoredFails

  def sequential(subtasks: Seq[WorkflowSubtask]): SequentialRunner =
    new SequentialRunner(subtasks)

  def parallel(subtasks: Seq[WorkflowSubtask], concurrency: Int): ParallelRunner =
    new ParallelRunner(subtasks, concurrency)

  def retryable(subtasks: Seq[WorkflowSubtask], maxRetries: Int): RetrySequentialRunner =
    new RetrySequentialRunner(subtasks, maxRetries)

  def resilient(
    subtasks: Seq[WorkflowSubtask],
    concurrency: Int,
    maxRetries: Int
  ): ResilientParallelRunner =
    new ResilientParallelRunner(subtasks, concurrency, maxRetries)
}
