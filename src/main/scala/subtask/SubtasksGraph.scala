package dev.fb.dbzpark
package subtask

import scala.collection.mutable

/**
 * A thread-safe directed acyclic graph (DAG) that tracks subtask execution order and state.
 *
 * Nodes are processed via Kahn's algorithm: callers repeatedly fetch zero-in-degree nodes, execute
 * them, and finalize them. Finalization either decrements the in-degree of dependent nodes (on
 * success) or recursively skips them (on failure or skip).
 *
 * Instances are constructed exclusively through [[SubtasksGraph.Builder]].
 */
class SubtasksGraph private (
  private val adjacencyListIds: Map[String, Set[String]],
  private var nodesQueueMap: mutable.HashMap[String, SubtaskNode]
) {
  private val finalizedNodes = new mutable.ArrayBuffer[SubtaskNode]
  private val lock           = new AnyRef

  /**
   * Returns the immutable adjacency list mapping each task ID to the set of its direct children.
   */
  def getAdjacencyList: Map[String, Set[String]] = lock.synchronized {
    adjacencyListIds
  }

  /**
   * Returns a snapshot of nodes still pending finalization (i.e. not yet removed from the graph).
   */
  def getNodesQueue: Seq[SubtaskNode] = lock.synchronized {
    nodesQueueMap.values.toSeq
  }

  /**
   * Returns true if there are nodes still pending finalization, false if the graph has fully drained.
   */
  def nonEmpty: Boolean = lock.synchronized {
    nodesQueueMap.nonEmpty
  }

  /**
   * Returns all nodes with no remaining dependencies, sorted by localPriority descending.
   *
   * @throws IllegalArgumentException if no zero-in-degree nodes exist but the queue is non-empty, indicating a cycle.
   */
  def getZeroInDegree: Seq[SubtaskNode] = lock.synchronized {
    val zeroInDegree = nodesQueueMap.values.filter(_.inDegree == 0).toVector

    if (zeroInDegree.isEmpty)
      require(nodesQueueMap.isEmpty, "The graph is not a DAG")

    zeroInDegree.sortBy(_.subtask.localPriority)(Ordering.Int.reverse)
  }

  /**
   * Marks a node as finalized and updates the graph state accordingly.
   *
   * The node must be in a terminal state (SUCCEEDED, FAILED, or SKIPPED):
   *   - SUCCEEDED: decrements the in-degree of all direct children.
   *   - FAILED / SKIPPED: recursively marks all descendants as SKIPPED.
   *
   * The node is removed from the queue after processing.
   *
   * @throws IllegalArgumentException if the node state is not a valid finalization state.
   */
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
      nodesQueueMap.get(childId).foreach { child =>
        nodesQueueMap(childId) = child.decrementInDegree
      }
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

  /**
   * Returns a tree-like ASCII representation of the pending nodes, rooted at all nodes with
   * in-degree 0 and sorted by localPriority descending at every level.
   *
   * Because the graph is a DAG rather than a strict tree, a node reachable from multiple parents
   * is printed in full the first time it is encountered; subsequent appearances are marked with
   * `(↑)` to avoid repeating the subtree.
   *
   * Example output:
   * {{{
   * === Subtasks Graph ===
   *
   * taskA [LP:10, WAITING]
   * ├── taskC [LP:5, WAITING]
   * │   └── taskE [LP:1, WAITING]
   * └── taskD [LP:3, WAITING]
   * taskB [LP:7, WAITING]
   * └── taskC [LP:5, WAITING] (↑)
   * }}}
   */
  override def toString: String = {
    val sb      = new StringBuilder
    val visited = mutable.HashSet[String]()

    def renderChildren(nodeId: String, prefix: String): Unit = {
      val children = adjacencyListIds
        .getOrElse(nodeId, Set())
        .flatMap(nodesQueueMap.get)
        .toSeq
        .sortBy(_.subtask.localPriority)(Ordering.Int.reverse)

      children.zipWithIndex.foreach { case (child, i) =>
        val isLast      = i == children.size - 1
        val connector   = if (isLast) "└── " else "├── "
        val alreadySeen = visited.contains(child.getSubtaskId)
        val seenNote    = if (alreadySeen) " (↑)" else ""

        sb.append(
          s"$prefix$connector${child.subtask.taskId} [LP:${child.subtask.localPriority}, ${child.state}]$seenNote\n"
        )

        if (!alreadySeen) {
          visited.add(child.getSubtaskId)
          renderChildren(child.getSubtaskId, prefix + (if (isLast) "    " else "│   "))
        }
      }
    }

    sb.append("=== Subtasks Graph ===\n\n")

    val roots = nodesQueueMap.values
      .filter(_.inDegree == 0)
      .toSeq
      .sortBy(_.subtask.localPriority)(Ordering.Int.reverse)

    roots.foreach { root =>
      visited.add(root.getSubtaskId)
      sb.append(s"${root.subtask.taskId} [LP:${root.subtask.localPriority}, ${root.state}]\n")
      renderChildren(root.getSubtaskId, "")
    }

    sb.toString
  }
}

object SubtasksGraph {

  /**
   * Builds a [[SubtasksGraph]] by registering subtasks and their dependencies.
   *
   * Usage:
   * {{{
   *   SubtasksGraph.Builder()
   *     .addSubtask(taskA)
   *     .addSubtask(taskB)
   *     .addDependency(ParentChildDependency(taskA.taskId, taskB.taskId))
   *     .build
   * }}}
   */
  class Builder private {
    private val nodesLookup   = new mutable.HashMap[String, SubtaskNode]()
    private val adjacencyList = new mutable.HashMap[String, mutable.HashSet[String]]()

    /**
     * Registers a subtask as a node in the graph. Must be called before establishing dependencies.
     *
     * @throws IllegalArgumentException if a subtask with the same taskId is already registered.
     */
    def addSubtask(subtask: WorkflowSubtask): Builder = {
      require(!adjacencyList.contains(subtask.taskId), s"Node with id {${subtask.taskId}} already exists")

      adjacencyList.put(subtask.taskId, new mutable.HashSet[String]())
      nodesLookup.put(subtask.taskId, SubtaskNode(subtask, inDegree = 0))

      this
    }

    /**
     * Adds a parent → child dependency, incrementing the child's in-degree.
     * Both nodes must already be registered via [[addSubtask]].
     *
     * @throws IllegalArgumentException if either node has not been registered.
     */
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

    /**
     * Convenience method that calls [[addDependency]] for each entry in `dependencies`.
     */
    def addDependencies(dependencies: Seq[ParentChildDependency]): Builder = {
      dependencies.foreach(addDependency)

      this
    }

    /**
     * Validates the graph is a DAG and returns the built [[SubtasksGraph]].
     *
     * @throws IllegalArgumentException if the graph is empty or contains a cycle.
     */
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
