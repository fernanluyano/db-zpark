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

  test("sample") {
    val builder = SubtasksGraph.Builder()
    for (i <- 1 to 7) yield builder.addSubtask(new MyTask(i.toString))

    val edges = Seq(
      ParentChildDependency("1", "2"),
      ParentChildDependency("2", "4"),
      ParentChildDependency("4", "3"),
      ParentChildDependency("4", "6"),
      ParentChildDependency("3", "5"),
      ParentChildDependency("5", "7"),
      ParentChildDependency("6", "7")
    )

    val graph = builder.addDependencies(edges).build
    val expectedAdjList = Map(
      "1" -> List(SubtaskNode(new MyTask("2"), 1)),
      "2" -> List(SubtaskNode(new MyTask("4"), 1)),
      "3" -> List(SubtaskNode(new MyTask("5"), 1)),
      "4" -> List(SubtaskNode(new MyTask("6"), 1), SubtaskNode(new MyTask("3"), 1)),
      "5" -> List(SubtaskNode(new MyTask("7"), 2)),
      "6" -> List(SubtaskNode(new MyTask("7"), 2)),
      "7" -> List(),
    )

    assertResult(expectedAdjList)(graph.adjacencyList)

    // the only node with no dependencies
    assertResult(SubtaskNode(new MyTask("1"), 0))(graph.topologicalSort.head)
    // the only node with 2 dependencies
    assertResult(SubtaskNode(new MyTask("7"), 2))(graph.topologicalSort(6))

    // the rest have exactly 1 dependency, and they can be in any order
    for (i <- 2 to 6){
      assert(graph.topologicalSort.contains(SubtaskNode(new MyTask(i.toString), 1)))
    }

    println(graph)
  }
}
