package dev.fb.dbzpark
package subtask

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SubtasksGraph private (
  private val adjacencyList: mutable.HashMap[String, Vector[SubtaskNode]],
  private var topologicalSort: Vector[SubtaskNode]
) {
  private val finalizedNodes = Vector.empty[SubtaskNode]
  private val lock           = new AnyRef

  def getAdjacencyList: Map[String, Seq[SubtaskNode]] = lock.synchronized {
    adjacencyList.toMap
  }

  def getSortedDAG: Seq[SubtaskNode] = lock.synchronized {
    topologicalSort.toSeq
  }

  def isEmpty: Boolean = lock.synchronized {
    topologicalSort.isEmpty
  }

  def getZeroInDegree: Seq[SubtaskNode] = lock.synchronized {
    topologicalSort.filter(_.getInDegree == 0)
  }

  def finalizeNode(node: SubtaskNode, state: NodeState): Unit = lock.synchronized {
    node.setState(state)
    state match {
      case SUCCEEDED        => decrementChildrenInDegree(node)
      case FAILED | SKIPPED => skipChildren(node)
      case _                => throw new IllegalArgumentException(s"Invalid finalization state: $state")
    }
    removeNode(node)
  }

  private def skipChildren(node: SubtaskNode): Unit =
    adjacencyList
      .getOrElse(node.subtask.taskId, Vector.empty[SubtaskNode])
      .foreach { child =>
        if (child.getState != SKIPPED) {
          child.setState(SKIPPED)
          skipChildren(child)
        }
        removeNode(child)
      }

  private def decrementChildrenInDegree(node: SubtaskNode): Unit = {
    val childrenIds = adjacencyList
      .getOrElse(node.subtask.taskId, Vector.empty[SubtaskNode])
      .map(ch => ch.subtask.taskId)
      .toSet

    if (childrenIds.nonEmpty) {
      topologicalSort.foreach { node =>
        if (childrenIds.contains(node.subtask.taskId))
          node.decrementInDegree
      }
    }
  }

  /* internal use, not thread safe
   * we only care about removing from the topologicalSort, not the adjacencyList, that's only for lookup purposes */
  private def removeNode(node: SubtaskNode): Unit =
    topologicalSort = topologicalSort.filterNot(_.subtask.taskId == node.subtask.taskId)

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

      val childNode = nodesLookup(dependency.childTaskId)

      childNode.incrementInDegree()
      nodesLookup.put(dependency.childTaskId, childNode)

      this
    }
    def addDependencies(dependencies: Seq[ParentChildDependency]): Builder = {
      dependencies.foreach(addDependency)

      this
    }

    def build: SubtasksGraph = {
      require(nodesLookup.nonEmpty, "The graph is empty!")

      val topologicalSort = getTopologicalSorted.map { node =>
        val savedInDegree = nodesLookup(node.subtask.taskId).getInDegree
        node.copy(inDegree = savedInDegree)
      }.toVector
      val adjList = adjacencyList.map { case (nodeId, neighbours) =>
        nodeId -> neighbours.map(nodesLookup).toVector
      }

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

    private def popNodesWithZeroInDegree(nodes: mutable.HashMap[String, SubtaskNode]): Seq[SubtaskNode] = {
      val zeroInDegreeNodes = nodes.filter { case (_, node) => node.getInDegree == 0 }

      zeroInDegreeNodes.keys.foreach { parentId =>
        adjacencyList(parentId).foreach { childId =>
          val childNode = nodes(childId)
          childNode.decrementInDegree
          nodes.update(childId, childNode)
        }

        nodes.remove(parentId)
      }

      zeroInDegreeNodes.values.toSeq
    }
  }

  object Builder {
    def apply(): Builder = new Builder
  }
}
