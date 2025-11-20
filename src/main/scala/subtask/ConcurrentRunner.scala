package dev.fb.dbzpark
package subtask

import subtask.ConcurrentRunner.{GROUP_DEPENDENCIES, NO_DEPENDENCIES, Strategy}

import zio.{Executor, ZIO}

/**
 * Executes subtasks concurrently with configurable parallelism and grouping strategies.
 *
 * This runner supports two execution strategies:
 *   - NO_DEPENDENCIES: All subtasks run concurrently (limited by maxRunning)
 *   - GROUP_DEPENDENCIES: Tasks within a group run concurrently, groups execute sequentially
 * @param subtasks
 *   The subtasks to execute
 * @param strategy
 *   Execution strategy (NO_DEPENDENCIES or GROUP_DEPENDENCIES)
 * @param maxRunning
 *   Maximum number of subtasks to run concurrently (controls fiber-level parallelism)
 */
class ConcurrentRunner private (val subtasks: Seq[WorkflowSubtask], val strategy: Strategy, val maxRunning: Int)
    extends SubtasksRunner {

  /**
   * Executes subtasks according to the configured strategy.
   *
   * Parallelism is limited to the maxRunning level. An optional executor can be provided to control which thread pool
   * runs the tasks.
   *
   * @param executor
   *   Optional executor for running tasks on a specific thread pool
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

  /**
   * Executes subtasks within a single group concurrently.
   *
   * All tasks in the group run in parallel (limited by maxRunning). If an executor is provided, tasks run on that
   * executor's thread pool.
   *
   * @param groupId
   *   Identifier for the group (used in logging)
   * @param groupedSubtasks
   *   Subtasks belonging to this group
   * @param executor
   *   Optional executor for running tasks
   * @return
   *   A ZIO effect that completes when all tasks in the group finish
   */
  private def runGroup(
    groupId: String,
    groupedSubtasks: Seq[WorkflowSubtask],
    executor: Option[Executor]
  ): ZIO[TaskEnvironment, Throwable, Unit] = {
    val effect = ZIO.foreachParDiscard(groupedSubtasks)(runOne).withParallelism(maxRunning)

    ZIO.logInfo(s"Running group $groupId") *>
      executor.fold(effect)(effect.onExecutor(_))
  }

  /**
   * Splits subtasks into groups based on their GroupingContext.
   *
   * Groups are sorted alphabetically by groupId to ensure deterministic execution order.
   *
   * @return
   *   Sequence of (groupId, subtasks) tuples sorted by groupId
   */
  private def splitInGroups: Seq[(String, Seq[WorkflowSubtask])] =
    subtasks.collect { case st if st.getContext.isInstanceOf[GroupingContext] => st }
      .groupBy(_.getContext.asInstanceOf[GroupingContext].groupId)
      .toSeq
      .sortBy(_._1)
}

object ConcurrentRunner {

  /**
   * Execution strategy for concurrent runner.
   */
  sealed trait Strategy

  /**
   * All subtasks run concurrently with no grouping.
   *
   * Use this when tasks have no dependencies on each other and can safely execute in parallel.
   */
  case object NO_DEPENDENCIES extends Strategy

  /**
   * Subtasks are grouped, with groups executing sequentially and tasks within a group executing concurrently.
   *
   * Use this when you have sets of independent tasks (within groups) but dependencies between groups. All subtasks
   * must have GroupingContext when using this strategy.
   */
  case object GROUP_DEPENDENCIES extends Strategy

  /**
   * Creates a new concurrent runner with validation.
   *
   * Validates that all subtasks have GroupingContext if using GROUP_DEPENDENCIES strategy.
   *
   * @param subtasks
   *   The subtasks to execute
   * @param strategy
   *   Execution strategy
   * @param maxRunningSubtasks
   *   Maximum number of subtasks to run concurrently (default: 4)
   * @return
   *   A new ConcurrentRunner instance
   * @throws IllegalArgumentException
   *   if GROUP_DEPENDENCIES is used with non-GroupingContext subtasks
   */
  def apply(subtasks: Seq[WorkflowSubtask], strategy: Strategy, maxRunningSubtasks: Int = 4): ConcurrentRunner = {
    validateStrategy(subtasks, strategy)
    new ConcurrentRunner(subtasks, strategy, maxRunningSubtasks)
  }

  /**
   * Validates that subtasks have appropriate context for the chosen strategy.
   *
   * @param subtasks
   *   The subtasks to validate
   * @param strategy
   *   The execution strategy
   * @throws IllegalArgumentException
   *   if GROUP_DEPENDENCIES is used with subtasks lacking GroupingContext
   */
  private def validateStrategy(subtasks: Seq[WorkflowSubtask], strategy: Strategy): Unit =
    if (strategy == GROUP_DEPENDENCIES) {
      val invalidSubtasks = subtasks.filterNot(_.getContext.isInstanceOf[GroupingContext])
      require(
        invalidSubtasks.isEmpty,
        s"All subtasks must have GroupingContext. Invalid: ${invalidSubtasks.map(_.getContext.name)}"
      )
    }
}
