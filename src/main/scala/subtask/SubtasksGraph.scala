package dev.fb.dbzpark
package subtask

import scala.collection.mutable

class SubtasksGraph private (
  private val adjacencyListIds: Map[String, Set[String]],
  private var nodesQueueMap: mutable.HashMap[String, SubtaskNode]
) {
  private val finalizedNodes = new mutable.ArrayBuffer[SubtaskNode]
  private val lock           = new AnyRef

  def getAdjacencyList: Map[String, Set[String]] = lock.synchronized {
    adjacencyListIds
  }

  def getNodesQueue: Seq[SubtaskNode] = lock.synchronized {
    nodesQueueMap.values.toSeq
  }

  def nonEmpty: Boolean = lock.synchronized {
    nodesQueueMap.nonEmpty
  }

  def getZeroInDegree: Seq[SubtaskNode] = lock.synchronized {
    val zeroInDegree = nodesQueueMap.values.filter(_.inDegree == 0).toVector

    if (zeroInDegree.isEmpty)
      require(nodesQueueMap.isEmpty, "The graph is not a DAG")

    zeroInDegree.sortBy(_.subtask.localPriority)(Ordering.Int.reverse)
  }

  def finalizeNode(node: SubtaskNode): Unit = lock.synchronized {
    nodesQueueMap(node.getSubtaskId) = node
    finalizedNodes.append(node)

    node.state match {
      case SUCCEEDED        => decrementChildrenInDegree(node)
      case FAILED | SKIPPED => skipChildren(node)
      case _                => throw new IllegalArgumentException(s"Invalid finalization state: ${node.state}")
    }

    nodesQueueMap.remove(node.getSubtaskId)
  }

  private def decrementChildrenInDegree(node: SubtaskNode): Unit = {
    val children = adjacencyListIds.getOrElse(node.getSubtaskId, Set())

    children.foreach { childId =>
      val newChild = nodesQueueMap(childId).decrementInDegree
      nodesQueueMap(childId) = newChild
    }
  }

  private def skipChildren(node: SubtaskNode): Unit = {
    val children = adjacencyListIds.getOrElse(node.getSubtaskId, Set())

    children.foreach { childId =>
      val childNode = nodesQueueMap(childId).setState(SKIPPED)

      if (nodesQueueMap.contains(childNode.getSubtaskId)) {
        finalizedNodes.append(childNode)
        nodesQueueMap.remove(childNode.getSubtaskId)
      }

      skipChildren(childNode)
    }
  }

  override def toString: String = {
    val sb = new StringBuilder

    sb.append("=== Subtasks Graph (Topological Order) ===\n\n")

    nodesQueueMap.values.foreach { node =>
      val taskId   = node.subtask.taskId
      val priority = node.subtask.localPriority
      val children = adjacencyListIds.getOrElse(taskId, Set.empty)

      if (children.isEmpty) {
        sb.append(s"$taskId [LP:$priority]\n")
      } else {
        val chMeta = children.map { ch =>
          s"[id: ${ch}, inDegree: ${nodesQueueMap(ch).inDegree}]"
        }.mkString("{ ", ", ", " }")
        sb.append(s"id: ${taskId}, localPriority: $priority ──> $chMeta\n")
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

      val childNode = nodesLookup(dependency.childTaskId).incrementInDegree
      nodesLookup.put(dependency.childTaskId, childNode)

      this
    }
    def addDependencies(dependencies: Seq[ParentChildDependency]): Builder = {
      dependencies.foreach(addDependency)

      this
    }

    def build: SubtasksGraph = {
      require(nodesLookup.nonEmpty, "The graph is empty!")

      val adjList = adjacencyList.mapValues(_.toSet).toMap
      testIsDAG(adjList)

      new SubtasksGraph(adjList, nodesLookup.clone)
    }

    private def testIsDAG(adjList: Map[String, Set[String]]): Unit = {
      val testQueue       = nodesLookup.clone
      val unverifiedGraph = new SubtasksGraph(adjList, testQueue)
      var done            = false

      while (!done) {
        val zeroInDegree = unverifiedGraph.getZeroInDegree
        zeroInDegree.foreach(n => unverifiedGraph.finalizeNode(n.setState(RUNNING).setState(SUCCEEDED)))

        done = zeroInDegree.isEmpty
      }
    }
  }

  object Builder {
    def apply(): Builder = new Builder
  }
}
