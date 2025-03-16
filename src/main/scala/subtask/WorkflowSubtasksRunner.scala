package dev.fb.dbzpark
package subtask

import zio.ZIO

sealed trait WorkflowSubtasksRunner[T] {
  protected val subtasks: Seq[WorkflowSubtask]
  protected val config: SubtasksRunnerConfig

  def run: ZIO[TaskEnvironment, Throwable, Unit]
}

class SequentialRunner[T] private (
  override protected val subtasks: Seq[WorkflowSubtask],
  override protected val config: SubtasksRunnerConfig
) extends WorkflowSubtasksRunner[T] {

  override def run: ZIO[TaskEnvironment, Throwable, Unit] =
    for {
      _ <- ZIO.foreachDiscard(subtasks)(_.run)
    } yield ()
}

object SequentialRunner {
  def apply[T](subtasks: Seq[WorkflowSubtask], failFast: Boolean = true, maxRetries: Int = 0): SequentialRunner[T] = {
    val config = SubtasksRunnerConfig(concurrency = 1, failFast = failFast, maxRetries = maxRetries)
    new SequentialRunner[T](subtasks, config)
  }
}

class ConcurrentRunner[T] private (
  override protected val subtasks: Seq[WorkflowSubtask],
  override protected val config: SubtasksRunnerConfig
) extends WorkflowSubtasksRunner[T] {

  override def run: ZIO[TaskEnvironment, Throwable, Unit] =
    for {
      _ <- ZIO.foreachParDiscard(subtasks)(_.run)
    } yield ()

}

object ConcurrentRunner {
  def apply[T](
    subtasks: Seq[WorkflowSubtask],
    concurrency: Int,
    failFast: Boolean = true,
    maxRetries: Int = 0
  ): ConcurrentRunner[T] = {
    require(concurrency > 1, s"concurrency must be > 1: supplied [${concurrency}]")
    val config = SubtasksRunnerConfig(concurrency = concurrency, failFast = failFast, maxRetries = maxRetries)
    new ConcurrentRunner[T](subtasks, config)
  }
}
