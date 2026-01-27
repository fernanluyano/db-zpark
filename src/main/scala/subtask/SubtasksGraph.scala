package dev.fb.dbzpark
package subtask

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SubtasksGraph private (
  val adjacencyList: Map[String, List[SubtaskNode]],
  val topologicalSort: List[SubtaskNode]
) {
  override def toString: String = {
    val sb = new StringBuilder

    sb.append("=== Subtasks Graph (Topological Order, LP <=> Local Priority) ===\n\n")

    topologicalSort.foreach { node =>
      val taskId   = node.subtask.taskId
      val priority = node.subtask.localPriority

      val children = adjacencyList
        .getOrElse(taskId, List.empty)
        .map(_.subtask.taskId)

      if (children.isEmpty) {
        sb.append(s"$taskId [LP:$priority]\n")
      } else {
        sb.append(s"$taskId [LP:$priority] ──> ${children.mkString(", ")}\n")
      }
    }

    sb.toString
  }


}

object SubtasksGraph {

  class Builder private {
    private val nodesLookup   = new mutable.HashMap[String, SubtaskNode]()
    private val adjacencyList = new mutable.HashMap[String, mutable.HashSet[String]]()

    def addSubtask(subtask: WorkflowSubtask): Builder = {
      require(!adjacencyList.contains(subtask.taskId), s"Node with id {${subtask.taskId}} already exists")

      adjacencyList.put(subtask.taskId, new mutable.HashSet[String]())
      nodesLookup.put(subtask.taskId, SubtaskNode(subtask, inDegree = 0))

      this
    }

    def addDependency(dependency: ParentChildDependency): Builder = {
      require(
        adjacencyList.contains(dependency.parentTaskId) && adjacencyList.contains(dependency.childTaskId),
        "Parent and child nodes must first exist before establishing a dependency. Add them first => addSubtask(...)"
      )
      adjacencyList(dependency.parentTaskId).addOne(dependency.childTaskId)

      val updatedNode = nodesLookup(dependency.childTaskId).incrementInDegree
      nodesLookup.put(dependency.childTaskId, updatedNode)

      this
    }
    def addDependencies(dependencies: Seq[ParentChildDependency]): Builder = {
      dependencies.foreach(addDependency)

      this
    }

    def build: SubtasksGraph = {
      require(nodesLookup.nonEmpty, "The graph is empty!")

      val topologicalSort = getTopologicalSorted.map { node =>
        val savedInDegree = nodesLookup(node.subtask.taskId).inDegree
        node.copy(inDegree = savedInDegree)
      }
      val adjList = adjacencyList.map { case (nodeId, neighbours) =>
        nodeId -> neighbours.map(nodesLookup).toList
      }.toMap

      new SubtasksGraph(adjList, topologicalSort)
    }

    private def getTopologicalSorted: List[SubtaskNode] = {
      val topologicalSort = new ArrayBuffer[SubtaskNode](nodesLookup.size)
      val nodesCopy       = nodesLookup.clone()

      while (nodesCopy.nonEmpty) {
        val zeroDegreeNodes = popNodesWithZeroInDegree(nodesCopy).sortBy(_.subtask.localPriority)(Ordering.Int.reverse)

        require(zeroDegreeNodes.nonEmpty, "The graph is not a DAG.")

        topologicalSort.addAll(zeroDegreeNodes)
      }

      topologicalSort.toList
    }

    private def popNodesWithZeroInDegree(nodes: mutable.HashMap[String, SubtaskNode]): List[SubtaskNode] = {
      val toPop = nodes.filter { case (_, node) => node.inDegree == 0 }

      toPop.keys.foreach { parentId =>
        adjacencyList(parentId).foreach { childId =>
          val updatedChildNode = nodes(childId).decrementInDegree
          nodes.update(childId, updatedChildNode)
        }

        nodes.remove(parentId)
      }

      toPop.values.toList
    }
  }

  object Builder {
    def apply(): Builder = new Builder
  }
}
