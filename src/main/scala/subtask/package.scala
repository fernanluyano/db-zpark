package dev.fb.dbzpark

import org.apache.hadoop.shaded.org.xbill.DNS.dnssec.R

/**
 * Core types and utilities for the subtask workflow framework.
 */
package object subtask {

  private[subtask] case class SubtaskNode(
    subtask: WorkflowSubtask,
    inDegree: Int,
    state: NodeState = WAITING
  ) {
    def incrementInDegree: SubtaskNode = copy(inDegree = inDegree + 1)

    def decrementInDegree: SubtaskNode = copy(inDegree = inDegree - 1)

    def setState(newState: NodeState): SubtaskNode = {
      validateTransition(newState)

      copy(state = newState)
    }

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

  case class ParentChildDependency(parentTaskId: String, childTaskId: String)

  sealed trait NodeState

  case object WAITING   extends NodeState
  case object RUNNING   extends NodeState
  case object SUCCEEDED extends NodeState
  case object FAILED    extends NodeState
  case object SKIPPED   extends NodeState
}
