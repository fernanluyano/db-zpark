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
      "1" -> List(SubtaskNode(new MyTask("2"), 1)),
      "2" -> List(SubtaskNode(new MyTask("4"), 1)),
      "3" -> List(SubtaskNode(new MyTask("5"), 1)),
      "4" -> List(SubtaskNode(new MyTask("6"), 1), SubtaskNode(new MyTask("3"), 1)),
      "5" -> List(SubtaskNode(new MyTask("7"), 2)),
      "6" -> List(SubtaskNode(new MyTask("7"), 2)),
      "7" -> List()
    )

    assertResult(expectedAdjList)(graph.getAdjacencyList)

    // the only node with no dependencies
    assertResult(SubtaskNode(new MyTask("1"), 0))(graph.getSortedDAG.head)
    // the only node with 2 dependencies
    assertResult(SubtaskNode(new MyTask("7"), 2))(graph.getSortedDAG(6))

    // the rest have exactly 1 dependency, and they can be in any order
    for (i <- 2 to 6)
      assert(graph.getSortedDAG.contains(SubtaskNode(new MyTask(i.toString), 1)))

    println(graph)
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
      "1" -> List.empty[SubtaskNode],
      "2" -> List.empty[SubtaskNode],
      "3" -> List.empty[SubtaskNode]
    )
    // expect the 3 nodes to have no connections
    val expectedDAG = Seq(
      SubtaskNode(new MyTask("1"), 0),
      SubtaskNode(new MyTask("2"), 0),
      SubtaskNode(new MyTask("3"), 0)
    )

    assertResult(expectedAdjListNoEdges)(graph.getAdjacencyList)
    assertResult(expectedDAG)(graph.getSortedDAG)
  }
}
