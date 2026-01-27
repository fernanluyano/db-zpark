package dev.fb.dbzpark

/**
 * Core types and utilities for the subtask workflow framework.
 */
package object subtask {

  private[subtask] case class SubtaskNode(subtask: WorkflowSubtask, inDegree: Int) {
    def incrementInDegree: SubtaskNode = this.copy(inDegree = this.inDegree + 1)

    def decrementInDegree: SubtaskNode = this.copy(inDegree = this.inDegree - 1)


  }

  case class ParentChildDependency(parentTaskId: String, childTaskId: String)
}
