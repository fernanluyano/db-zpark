package dev.fb.dbzpark
package subtask

import subtask.ConcurrentRunner.Strategy

import zio.{Executor, ZIO}

import java.util.concurrent.Executors

class ExecutionModel private (val runner: SubtasksRunner, val executor: Option[Executor]) {
  def run: ZIO[TaskEnvironment, Throwable, Unit] = runner.run(executor)
}

object ExecutionModel {

  private lazy val defaultExecutor = Executor.fromJavaExecutor(
    Executors.newFixedThreadPool(sys.env.getOrElse("NUM_THREADS", "4").toInt)
  )

  def singleton(subtask: WorkflowSubtask): ExecutionModel =
    sequential(Seq(subtask))

  def sequential(subtasks: Seq[WorkflowSubtask]): ExecutionModel = {
    val runner = SequentialRunner(subtasks)
    new ExecutionModel(runner, None)
  }

  def concurrent(
    subtasks: Seq[WorkflowSubtask],
    strategy: Strategy,
    executor: Executor = defaultExecutor
  ): ExecutionModel = {
    val runner = ConcurrentRunner(subtasks, strategy)
    new ExecutionModel(runner, Some(executor))
  }

}
