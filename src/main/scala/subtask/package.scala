package dev.fb.dbzpark

import org.apache.hadoop.shaded.org.xbill.DNS.dnssec.R

/**
 * Core types and utilities for the subtask workflow framework.
 */
package object subtask {

  /**
   * An immutable graph node that wraps a [[WorkflowSubtask]] with its scheduling metadata.
   *
   * `inDegree` tracks the number of pending parents; when it reaches 0 the node is eligible
   * for execution. State transitions are validated and must follow the allowed paths:
   *
   *   WAITING → RUNNING → SUCCEEDED
   *                     → FAILED
   *                     → SKIPPED
   *   WAITING → SKIPPED
   *
   * Instances are immutable — mutation methods return a new copy.
   */
  private[subtask] case class SubtaskNode(
    subtask: WorkflowSubtask,
    inDegree: Int,
    state: NodeState = WAITING
  ) {

    /**
     * Returns a copy of this node with `inDegree` incremented by one.
     */
    def incrementInDegree: SubtaskNode = copy(inDegree = inDegree + 1)

    /**
     * Returns a copy of this node with `inDegree` decremented by one.
     */
    def decrementInDegree: SubtaskNode = copy(inDegree = inDegree - 1)

    /**
     * Returns a copy of this node with the given state applied.
     *
     * @throws IllegalArgumentException if the transition from the current state to `newState` is not allowed.
     */
    def setState(newState: NodeState): SubtaskNode = {
      validateTransition(newState)

      copy(state = newState)
    }

    /**
     * Returns the task ID of the underlying [[WorkflowSubtask]].
     */
    def getSubtaskId: String = subtask.taskId

    private def validateTransition(to: NodeState): Unit = {
      val valid = (state, to) match {
        case (RUNNING, SUCCEEDED) => true
        case (RUNNING, FAILED)    => true
        case (RUNNING, SKIPPED)   => true
        case (WAITING, SKIPPED)   => true
        case (WAITING, RUNNING)   => true
        case _                    => false
      }
      require(valid, s"Invalid node state transition: $state -> $to")
    }
  }

  /**
   * Represents a directed dependency from `parentTaskId` to `childTaskId`, meaning the child
   * cannot be scheduled until the parent has completed successfully.
   */
  case class ParentChildDependency(parentTaskId: String, childTaskId: String)

  /**
   * The execution state of a [[SubtaskNode]] within the graph.
   * Transitions are enforced by [[SubtaskNode.setState]].
   */
  sealed trait NodeState

  /** The node is waiting for its dependencies to complete. */
  case object WAITING extends NodeState

  /** The node has been dispatched and is currently executing. */
  case object RUNNING extends NodeState

  /** The node completed without error. Its children will have their in-degree decremented. */
  case object SUCCEEDED extends NodeState

  /** The node raised an error. Its descendants will be marked [[SKIPPED]]. */
  case object FAILED extends NodeState

  /** The node was not executed because an ancestor failed or was itself skipped. */
  case object SKIPPED extends NodeState
}
