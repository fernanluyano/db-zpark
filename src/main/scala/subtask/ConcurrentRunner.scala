package dev.fb.dbzpark
package subtask

import subtask.ConcurrentRunner.{GROUP_DEPENDENCIES, NO_DEPENDENCIES, Strategy}

import zio.{Executor, ZIO}

class ConcurrentRunner private (val subtasks: Seq[WorkflowSubtask], val strategy: Strategy, val maxRunning: Int)
    extends SubtasksRunner {

  /**
   * Overrides the default execution to run subtasks concurrently. Limits parallelism to the configured concurrency
   * level.
   *
   * @return
   *   A ZIO effect that completes when all subtasks have been processed
   */
  override def run(executor: Option[Executor]): ZIO[TaskEnvironment, Throwable, Unit] = {
    def runAllGroups: ZIO[TaskEnvironment, Throwable, Unit] =
      ZIO.attempt(splitInGroups).flatMap { groups =>
        ZIO.foreachDiscard(groups) { case (groupId, groupSubtasks) =>
          runGroup(groupId, groupSubtasks, executor)
        }
      }

    strategy match {
      case NO_DEPENDENCIES    => runGroup("no-group", subtasks, executor)
      case GROUP_DEPENDENCIES => runAllGroups
    }
  }

  private def runGroup(
    groupId: String,
    groupedSubtasks: Seq[WorkflowSubtask],
    executor: Option[Executor]
  ): ZIO[TaskEnvironment, Throwable, Unit] = {
    val effect = ZIO.foreachParDiscard(groupedSubtasks)(runOne).withParallelism(maxRunning)

    ZIO.logInfo(s"Running group $groupId") *>
      executor.fold(effect)(effect.onExecutor(_))
  }

  private def splitInGroups: Seq[(String, Seq[WorkflowSubtask])] =
    subtasks.collect { case st if st.getContext.isInstanceOf[GroupingContext] => st }
      .groupBy(_.getContext.asInstanceOf[GroupingContext].groupId)
      .toSeq
      .sortBy(_._1)
}

object ConcurrentRunner {
  sealed trait Strategy
  case object NO_DEPENDENCIES    extends Strategy
  case object GROUP_DEPENDENCIES extends Strategy

  def apply(subtasks: Seq[WorkflowSubtask], strategy: Strategy, maxRunningSubtasks: Int = 4): ConcurrentRunner = {
    validateStrategy(subtasks, strategy)
    new ConcurrentRunner(subtasks, strategy, maxRunningSubtasks)
  }

  private def validateStrategy(subtasks: Seq[WorkflowSubtask], strategy: Strategy): Unit =
    if (strategy == GROUP_DEPENDENCIES) {
      val invalidSubtasks = subtasks.filterNot(_.getContext.isInstanceOf[GroupingContext])
      require(
        invalidSubtasks.isEmpty,
        s"All subtasks must have GroupingContext. Invalid: ${invalidSubtasks.map(_.getContext.name)}"
      )
    }
}
