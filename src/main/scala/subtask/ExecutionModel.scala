package dev.fb.dbzpark
package subtask

import zio.{Executor, ZIO}

import java.util.concurrent.Executors

class ExecutionModel private (val runner: SubtasksRunner, val executor: Executor) {
  def run: ZIO[TaskEnvironment, Throwable, Unit] = runner.run(executor)
}

object ExecutionModel {
  private lazy val defaultExecutor = Executor.fromJavaExecutor(
    Executors.newFixedThreadPool(sys.env.getOrElse("NUM_THREADS", "4").toInt)
  )
  private lazy val singleExecutor = Executor.fromJavaExecutor(
    Executors.newSingleThreadExecutor()
  )

  def singletonSubtask(subtask: WorkflowSubtask, executor: Executor = singleExecutor): ExecutionModel =
    sequential(Seq(subtask), executor)

  def sequential(subtasks: Seq[WorkflowSubtask], executor: Executor = singleExecutor): ExecutionModel = {
    val runner = SequentialRunner(subtasks)
    new ExecutionModel(runner, executor)
  }

  def concurrentNoDeps(subtasks: Seq[WorkflowSubtask], executor: Executor = defaultExecutor): ExecutionModel = {
    val runner = ConcurrentRunner(subtasks)
    new ExecutionModel(runner, executor)
  }
}
