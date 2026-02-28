package dev.fb.dbzpark
package subtask

import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite
import zio.{Task, ZIO}

class SubtasksGraphTest extends AnyFunSuite {
  class MyTask(override val taskId: String) extends WorkflowSubtask {

    override protected def readSource(env: TaskEnvironment): Task[Dataset[_]] = ???

    override protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] = ???

    override protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] = ZIO.unit
  }

  test("SubtasksGraph good graph test") {
    val builder = SubtasksGraph.Builder()
    for (i <- 1 to 7) yield builder.addSubtask(new MyTask(i.toString))

    val edges = Seq(
      ParentChildDependency("2", "4"),
      ParentChildDependency("4", "3"),
      ParentChildDependency("4", "6"),
      ParentChildDependency("3", "5"),
      ParentChildDependency("5", "7"),
      ParentChildDependency("6", "7")
    )

    // just to show single and several additions
    val graph = builder
      .addDependency(ParentChildDependency("1", "2"))
      .addDependencies(edges)
      .build
    val expectedAdjList = Map(
      "1" -> Set("2"),
      "2" -> Set("4"),
      "3" -> Set("5"),
      "4" -> Set("3", "6"),
      "5" -> Set("7"),
      "6" -> Set("7"),
      "7" -> Set.empty[String]
    )

    assertResult(expectedAdjList)(graph.getAdjacencyList)

    // the only node with no dependencies
    assertResult(SubtaskNode(new MyTask("1"), 0))(graph.getNodesQueue.filter(_.inDegree == 0).head)
    // the only node with 2 dependencies
    assertResult(SubtaskNode(new MyTask("7"), 2))(graph.getNodesQueue.filter(_.inDegree == 2).head)

    // the rest have exactly 1 dependency, and they can be in any order
    for (i <- 2 to 6)
      assert(graph.getNodesQueue.contains(SubtaskNode(new MyTask(i.toString), 1)))
  }

  test("SubtasksGraph DAG with cycle test") {
    val builder = SubtasksGraph.Builder()
    for (i <- 1 to 3) yield builder.addSubtask(new MyTask(i.toString))

    // this has a cycle
    val edges = Seq(
      ParentChildDependency("1", "2"),
      ParentChildDependency("2", "1")
    )

    assertThrows[IllegalArgumentException](builder.addDependencies(edges).build)
  }

  test("SubtasksGraph with disconnected DAG test") {
    val builder = SubtasksGraph.Builder()
    for (i <- 1 to 3) yield builder.addSubtask(new MyTask(i.toString))

    val graph = builder.build

    val expectedAdjListNoEdges = Map(
      "1" -> Set.empty[String],
      "2" -> Set.empty[String],
      "3" -> Set.empty[String]
    )
    // expect the 3 nodes to have no connections
    val expectedDAG = Set(
      SubtaskNode(new MyTask("1"), 0),
      SubtaskNode(new MyTask("2"), 0),
      SubtaskNode(new MyTask("3"), 0)
    )

    assertResult(expectedAdjListNoEdges)(graph.getAdjacencyList)
    assertResult(expectedDAG)(graph.getNodesQueue.toSet)
  }
}
