package dev.fb.dbzpark
package subtask

import zio.ZIO
import zio.Task

/**
 * Drives execution of a [[SubtasksGraph]] using a recursive Kahn's-algorithm loop.
 *
 * On each iteration, all currently ready nodes (in-degree == 0) are fetched from the graph.
 * Up to [[TaskEnvironment.maxConcurrentSubtasks]] of them are dispatched in parallel on
 * [[TaskEnvironment.subtasksExecutor]]. When the batch completes the loop recurses to pick up
 * any nodes that became ready as a result of those completions.
 *
 * Failure handling is governed by [[TaskEnvironment.failFast]]:
 *   - `true`  — the first failure propagates immediately as a ZIO failure, halting the run.
 *   - `false` — failures are logged and the graph continues draining; children of failed nodes
 *               are skipped by [[SubtasksGraph.finalizeNode]].
 *
 * Instances are created via [[SubtasksManager.apply]].
 */
final class SubtasksManager private (private val environment: TaskEnvironment) {

  /**
   * Runs all subtasks in the graph, respecting dependencies, concurrency limits, and the
   * fail-fast policy. Completes successfully when the graph has fully drained (all nodes
   * finalized or skipped), or fails immediately if `failFast` is enabled and a task fails.
   */
  def run: ZIO[TaskEnvironment, Throwable, Unit] =
    ZIO
      .attempt(environment.subtasksGraph.getZeroInDegree)
      .flatMap {
        case Seq() => ZIO.unit
        case nodes => runBatchAndWait(nodes)
      }
      .tapError(_ => ZIO.logError(environment.subtasksGraph.toStringError))

  /**
   * Runs up to maxConcurrentSubtasks nodes in parallel on the configured executor, then
   * recurses into `run` to process the next wave of newly unblocked nodes.
   * Nodes beyond the concurrency cap are not dropped — they will be picked up on the next
   * recursive call once the current batch has completed.
   */
  private def runBatchAndWait(nodes: Seq[SubtaskNode]): ZIO[TaskEnvironment, Throwable, Unit] =
    ZIO.logInfo(s"Running next batch") *>
      ZIO
        .foreachParDiscard(nodes.take(environment.maxConcurrentSubtasks))(node => runAndManage(node.setState(RUNNING)))
        .onExecutor(environment.subtasksExecutor) *> run

  /**
   * Logs the dispatch, runs the subtask, and routes the outcome to the appropriate handler.
   */
  private def runAndManage(subtaskNode: SubtaskNode) =
    for {
      _ <- ZIO.logInfo(s"Task scheduled for execution: ${subtaskNode.subtask}")
      _ <- subtaskNode.subtask.run.foldZIO(
             failure = e => handleNodeFailure(subtaskNode, e),
             success = _ => handleNodeSuccess(subtaskNode)
           )
    } yield ()

  /**
   * Finalizes the node as FAILED (causing the graph to skip its descendants), then either
   * re-raises the error (failFast=true) or logs it and continues (failFast=false).
   */
  private def handleNodeFailure(subtaskNode: SubtaskNode, e: Throwable): Task[Unit] =
    ZIO.attempt(environment.subtasksGraph.finalizeNode(subtaskNode.setState(FAILED))) *>
      (if (environment.failFast)
         ZIO.logError(s"Failing fast due to failure in ${subtaskNode.subtask.taskId}") *> ZIO.fail(e)
       else ZIO.logError(s"SubtaskNode failed: ${e.getMessage}"))

  /**
   * Finalizes the node as SUCCEEDED, which decrements the in-degree of its direct children
   * and makes them eligible for the next batch if all their other parents have also completed.
   */
  private def handleNodeSuccess(subtaskNode: SubtaskNode): Task[Unit] =
    ZIO.attempt(environment.subtasksGraph.finalizeNode(subtaskNode.setState(SUCCEEDED)))
}

object SubtasksManager {
  def apply(environment: TaskEnvironment): SubtasksManager =
    new SubtasksManager(environment)
}
