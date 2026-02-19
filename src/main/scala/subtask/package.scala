package dev.fb.dbzpark

/**
 * Core types and utilities for the subtask workflow framework.
 */
package object subtask {

  private[subtask] case class SubtaskNode(
    subtask: WorkflowSubtask,
    private var inDegree: Int,
    private var state: NodeState = WAITING
  ) {
    def incrementInDegree(): Unit = inDegree += 1

    def decrementInDegree(): Unit = inDegree -= 1

    def getInDegree: Int = inDegree

    def getState: NodeState = state

    def setState(newState: NodeState): Unit = {
      validateTransition(newState)
      state = newState
    }

    private def validateTransition(to: NodeState): Unit = {
      val valid = (state, to) match {
        case (RUNNING, SUCCEEDED) => true
        case (RUNNING, FAILED)    => true
        case (RUNNING, SKIPPED)   => true
        case (WAITING, SKIPPED)   => true
        case (READY, SKIPPED)     => true
        case _                    => false
      }
      require(valid, s"Invalid node state transition: $state -> $to")
    }
  }

  case class ParentChildDependency(parentTaskId: String, childTaskId: String)

  sealed trait NodeState

  case object WAITING   extends NodeState
  case object READY     extends NodeState
  case object RUNNING   extends NodeState
  case object SUCCEEDED extends NodeState
  case object FAILED    extends NodeState
  case object SKIPPED   extends NodeState
}
