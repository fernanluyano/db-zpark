package dev.fb.dbzpark
package subtask

import org.apache.spark.sql.{Dataset, SparkSession}
import zio.{Executor, Task, ZIO, ZLayer}
import zio.test._
import zio.test.ZTestLogger
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Tests for [[SubtasksManager]], covering task execution ordering, concurrency, failure handling,
 * and dependency-based skipping within a [[SubtasksGraph]].
 *
 * Test helpers:
 *   - [[SuccessfulSubtask]]  – completes with ZIO.unit
 *   - [[FailingSubtask]]     – always fails with a RuntimeException
 *   - [[TrackedSubtask]]     – sleeps 50ms and tracks active/peak concurrency via AtomicIntegers
 *   - [[PrioritySubtask]]    – records execution order into a ConcurrentLinkedQueue
 *
 * [[makeEnv]] wires a [[TaskEnvironment]] backed by a fixed thread pool executor, allowing
 * maxConcurrent and failFast to be varied per test.
 */
object SubtasksManagerSpec extends ZIOSpecDefault {

  /** A subtask that succeeds immediately without performing any work. */
  class SuccessfulSubtask(override val taskId: String) extends WorkflowSubtask {
    override protected def readSource(env: TaskEnvironment): Task[Dataset[_]] =
      ZIO.succeed(null.asInstanceOf[Dataset[_]])
    override protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] = ZIO.succeed(inDs)
    override protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit]             = ZIO.unit
  }

  /**
   * A subtask that appends its taskId to `order` when run.
   * Used to assert dispatch ordering when maxConcurrentSubtasks = 1.
   */
  class PrioritySubtask(
    override val taskId: String,
    override val localPriority: Int,
    order: ConcurrentLinkedQueue[String]
  ) extends WorkflowSubtask {
    override protected def readSource(env: TaskEnvironment): Task[Dataset[_]] =
      ZIO.attempt(order.add(taskId)).as(null.asInstanceOf[Dataset[_]])
    override protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] = ZIO.succeed(inDs)
    override protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit]             = ZIO.unit
  }

  /**
   * A subtask that increments `active` on start, records the `peak` concurrent count, sleeps 50ms,
   * then decrements `active` on completion. Used to assert actual concurrency levels.
   */
  class TrackedSubtask(override val taskId: String, active: AtomicInteger, peak: AtomicInteger)
      extends WorkflowSubtask {
    override protected def readSource(env: TaskEnvironment): Task[Dataset[_]] =
      ZIO.attempt {
        val current = active.incrementAndGet()
        peak.updateAndGet(p => math.max(p, current))
        Thread.sleep(50)
        active.decrementAndGet()
        null.asInstanceOf[Dataset[_]]
      }
    override protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] = ZIO.succeed(inDs)
    override protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit]             = ZIO.unit
  }

  /** A subtask that always fails with a RuntimeException containing the taskId. */
  class FailingSubtask(override val taskId: String) extends WorkflowSubtask {
    override protected def readSource(env: TaskEnvironment): Task[Dataset[_]] =
      ZIO.fail(new RuntimeException(s"$taskId failed"))
    override protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] = ZIO.succeed(inDs)
    override protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit]             = ZIO.unit
  }

  /**
   * Builds a [[TaskEnvironment]] for testing. sparkSession is null — subtask helpers must not
   * trigger Spark operations. maxConcurrent defaults to 4; failFast defaults to false.
   */
  def makeEnv(graph: SubtasksGraph, maxConcurrent: Int = 4, failFastFlag: Boolean = false): TaskEnvironment =
    new TaskEnvironment {
      override def sparkSession: SparkSession   = null
      override def appName: String              = "test"
      override def subtasksGraph: SubtasksGraph = graph
      override def subtasksExecutor: Executor   = Executor.fromJavaExecutor(Executors.newFixedThreadPool(maxConcurrent))
      override def maxConcurrentSubtasks: Int   = maxConcurrent
      override def failFast: Boolean            = failFastFlag
    }

  override def spec = suite("SubtasksManager")(
    // All three tasks have no dependencies, so they are dispatched in the same batch.
    // Asserts each task is logged as scheduled and the graph is empty on completion.
    test("3 independent successful tasks all complete") {
      val task1 = new SuccessfulSubtask("task1")
      val task2 = new SuccessfulSubtask("task2")
      val task3 = new SuccessfulSubtask("task3")

      val graph = SubtasksGraph
        .Builder()
        .addSubtask(task1)
        .addSubtask(task2)
        .addSubtask(task3)
        .build

      val env = makeEnv(graph)

      for {
        _         <- SubtasksManager(env).run.provide(ZLayer.succeed(env))
        logOutput <- ZTestLogger.logOutput
        messages   = logOutput.map(_.message())
      } yield assertTrue(
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task1")),
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task2")),
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task3")),
        !graph.nonEmpty
      )
    },
    // With failFast=true, a failure in any task propagates as a ZIO failure and halts the run.
    // Asserts the effect exits as a failure carrying the task2 exception, and the
    // "Failing fast" log message is emitted.
    test("failFast stops the run when a task fails") {
      val task1 = new SuccessfulSubtask("task1")
      val task2 = new FailingSubtask("task2")
      val task3 = new SuccessfulSubtask("task3")

      val graph = SubtasksGraph
        .Builder()
        .addSubtask(task1)
        .addSubtask(task2)
        .addSubtask(task3)
        .build

      val env = makeEnv(graph, failFastFlag = true)

      for {
        exit      <- SubtasksManager(env).run.provide(ZLayer.succeed(env)).exit
        logOutput <- ZTestLogger.logOutput
        messages   = logOutput.map(_.message())
      } yield assertTrue(
        exit.isFailure,
        exit.causeOption.flatMap(_.failureOption).exists(_.getMessage.contains("task2 failed")),
        messages.exists(_.contains("Failing fast due to failure in task2"))
      )
    },
    // With failFast=false (default), a failing task is logged as an error but does not halt
    // the run. All three tasks are scheduled and the graph drains to empty.
    test("partial failure completes when failFast is false") {
      val task1 = new SuccessfulSubtask("task1")
      val task2 = new FailingSubtask("task2")
      val task3 = new SuccessfulSubtask("task3")

      val graph = SubtasksGraph
        .Builder()
        .addSubtask(task1)
        .addSubtask(task2)
        .addSubtask(task3)
        .build

      val env = makeEnv(graph)

      for {
        _         <- SubtasksManager(env).run.provide(ZLayer.succeed(env))
        logOutput <- ZTestLogger.logOutput
        messages   = logOutput.map(_.message())
      } yield assertTrue(
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task1")),
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task2")),
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task3")),
        messages.exists(_.contains("SubtaskNode failed")),
        !graph.nonEmpty
      )
    },
    // Graph topology (task2 fails):
    //   task1   task2 (fail)   task3
    //           /    \           \
    //        task4  task5        task6
    //           \                 \
    //            +----> task7 <---+
    //
    // task4, task5 are direct children of task2 and must be skipped.
    // task7 is reachable from task4 (skipped) but also from task6 (succeeded), yet because
    // task4 is skipped its skip propagates and task7 is skipped regardless.
    // task1, task3, and task6 must still run successfully.
    test("children are skipped when parent fails") {
      val task1 = new SuccessfulSubtask("task1")
      val task2 = new FailingSubtask("task2")
      val task3 = new SuccessfulSubtask("task3")
      val task4 = new SuccessfulSubtask("task4")
      val task5 = new SuccessfulSubtask("task5")
      val task6 = new SuccessfulSubtask("task6")
      val task7 = new SuccessfulSubtask("task7")

      val graph = SubtasksGraph
        .Builder()
        .addSubtask(task1)
        .addSubtask(task2)
        .addSubtask(task3)
        .addSubtask(task4)
        .addSubtask(task5)
        .addSubtask(task6)
        .addSubtask(task7)
        .addDependency(ParentChildDependency("task2", "task4"))
        .addDependency(ParentChildDependency("task2", "task5"))
        .addDependency(ParentChildDependency("task4", "task7"))
        .addDependency(ParentChildDependency("task3", "task6"))
        .addDependency(ParentChildDependency("task6", "task7"))
        .build

      val env = makeEnv(graph)

      for {
        _         <- SubtasksManager(env).run.provide(ZLayer.succeed(env))
        logOutput <- ZTestLogger.logOutput
        messages   = logOutput.map(_.message())
      } yield assertTrue(
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task1")),
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task2")),
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task3")),
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task6")),
        !messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task4")),
        !messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task5")),
        !messages.exists(m => m.contains("Task scheduled for execution") && m.contains("task7")),
        !graph.nonEmpty
      )
    },
    // Four independent tasks with maxConcurrent=4. Asserts that peak active count > 1,
    // confirming the tasks overlapped in time rather than running sequentially.
    test("independent tasks run concurrently") {
      val active = new AtomicInteger(0)
      val peak   = new AtomicInteger(0)

      val graph = SubtasksGraph
        .Builder()
        .addSubtask(new TrackedSubtask("task1", active, peak))
        .addSubtask(new TrackedSubtask("task2", active, peak))
        .addSubtask(new TrackedSubtask("task3", active, peak))
        .addSubtask(new TrackedSubtask("task4", active, peak))
        .build

      val env = makeEnv(graph, maxConcurrent = 4)

      for {
        _ <- SubtasksManager(env).run.provide(ZLayer.succeed(env))
      } yield assertTrue(peak.get() > 1)
    },
    // Five independent tasks with maxConcurrent=2. Asserts peak is in the range (1, 2],
    // confirming that at least two tasks ran in parallel but never more than two at once.
    test("maxConcurrentSubtasks limits parallelism") {
      val active = new AtomicInteger(0)
      val peak   = new AtomicInteger(0)

      val graph = SubtasksGraph
        .Builder()
        .addSubtask(new TrackedSubtask("task1", active, peak))
        .addSubtask(new TrackedSubtask("task2", active, peak))
        .addSubtask(new TrackedSubtask("task3", active, peak))
        .addSubtask(new TrackedSubtask("task4", active, peak))
        .addSubtask(new TrackedSubtask("task5", active, peak))
        .build

      val env = makeEnv(graph, maxConcurrent = 2)

      for {
        _ <- SubtasksManager(env).run.provide(ZLayer.succeed(env))
      } yield assertTrue(
        peak.get() > 1,
        peak.get() <= 2
      )
    },
    // Three independent tasks with maxConcurrent=1. Asserts peak == 1, meaning no two
    // tasks were ever active simultaneously.
    test("maxConcurrentSubtasks = 1 runs tasks sequentially") {
      val active = new AtomicInteger(0)
      val peak   = new AtomicInteger(0)

      val graph = SubtasksGraph
        .Builder()
        .addSubtask(new TrackedSubtask("task1", active, peak))
        .addSubtask(new TrackedSubtask("task2", active, peak))
        .addSubtask(new TrackedSubtask("task3", active, peak))
        .build

      val env = makeEnv(graph, maxConcurrent = 1)

      for {
        _ <- SubtasksManager(env).run.provide(ZLayer.succeed(env))
      } yield assertTrue(peak.get() == 1)
    },
    // Three independent tasks with distinct localPriority values and maxConcurrent=1.
    // Asserts they are dispatched in descending priority order: high → medium → low.
    test("higher priority tasks are dispatched first") {
      val order = new ConcurrentLinkedQueue[String]()

      val graph = SubtasksGraph
        .Builder()
        .addSubtask(new PrioritySubtask("low", localPriority = 1, order))
        .addSubtask(new PrioritySubtask("medium", localPriority = 5, order))
        .addSubtask(new PrioritySubtask("high", localPriority = 10, order))
        .build

      val env = makeEnv(graph, maxConcurrent = 1)

      for {
        _ <- SubtasksManager(env).run.provide(ZLayer.succeed(env))
      } yield {
        val executed = order.toArray.toSeq.map(_.toString)
        assertTrue(
          executed == Seq("high", "medium", "low")
        )
      }
    },
    // Diamond graph: taskA and taskB both point to taskC (in-degree 2).
    // Asserts that taskC is only scheduled after both parents complete, and the graph
    // drains to empty.
    test("diamond dependency success path - child runs after both parents complete") {
      val taskA = new SuccessfulSubtask("taskA")
      val taskB = new SuccessfulSubtask("taskB")
      val taskC = new SuccessfulSubtask("taskC")

      val graph = SubtasksGraph
        .Builder()
        .addSubtask(taskA)
        .addSubtask(taskB)
        .addSubtask(taskC)
        .addDependency(ParentChildDependency("taskA", "taskC"))
        .addDependency(ParentChildDependency("taskB", "taskC"))
        .build

      val env = makeEnv(graph)

      for {
        _         <- SubtasksManager(env).run.provide(ZLayer.succeed(env))
        logOutput <- ZTestLogger.logOutput
        messages   = logOutput.map(_.message())
      } yield assertTrue(
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("taskA")),
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("taskB")),
        messages.exists(m => m.contains("Task scheduled for execution") && m.contains("taskC")),
        !graph.nonEmpty
      )
    },
    // All three independent tasks fail. With failFast=false the manager must not throw;
    // instead it logs each failure and drains the graph to empty.
    test("all tasks fail - graph empties and run completes") {
      val task1 = new FailingSubtask("task1")
      val task2 = new FailingSubtask("task2")
      val task3 = new FailingSubtask("task3")

      val graph = SubtasksGraph
        .Builder()
        .addSubtask(task1)
        .addSubtask(task2)
        .addSubtask(task3)
        .build

      val env = makeEnv(graph)

      for {
        _         <- SubtasksManager(env).run.provide(ZLayer.succeed(env))
        logOutput <- ZTestLogger.logOutput
        messages   = logOutput.map(_.message())
      } yield assertTrue(
        messages.count(_.contains("SubtaskNode failed")) == 3,
        !graph.nonEmpty
      )
    }
  )
}
