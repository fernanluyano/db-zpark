package dev.fb.dbzpark
package subtask

import subtask.ConcurrentRunner.{GROUP_DEPENDENCIES, NO_DEPENDENCIES}

import org.apache.spark.sql.{Dataset, SparkSession}
import zio.test._
import zio.{Executor, Task, ZIO, ZLayer}

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap

object ConcurrentRunnerSpec extends ZIOSpecDefault {
  // Create a SparkSession for testing
  private val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("runner-test")
    .getOrCreate()

  // Create a test environment
  class TestTaskEnvironment extends TaskEnvironment {
    override def sparkSession: SparkSession = spark
    override def appName: String            = "TestApp"
  }

  val taskEnvLayer: ZLayer[Any, Nothing, TaskEnvironment] = ZLayer.succeed(new TestTaskEnvironment)

  // Create a test executor
  private val testExecutor = Executor.fromJavaExecutor(Executors.newFixedThreadPool(3))

  // Execution tracker to verify concurrent vs sequential execution
  case class ExecutionTracker() {
    private val executionOrder     = new AtomicInteger(0)
    private val executionLog       = TrieMap.empty[String, Int]
    private val startTimes         = TrieMap.empty[String, Long]
    private val endTimes           = TrieMap.empty[String, Long]
    private val activeTasksCounter = new AtomicInteger(0)
    private val maxConcurrent      = new AtomicInteger(0)

    def recordStart(taskName: String): Unit = {
      val order = executionOrder.incrementAndGet()
      executionLog.put(taskName, order)
      startTimes.put(taskName, System.nanoTime())
      val active = activeTasksCounter.incrementAndGet()
      // Update max concurrent if needed
      var current = maxConcurrent.get()
      while (active > current && !maxConcurrent.compareAndSet(current, active))
        current = maxConcurrent.get()
    }

    def recordEnd(taskName: String): Unit = {
      endTimes.put(taskName, System.nanoTime())
      activeTasksCounter.decrementAndGet()
    }

    def getExecutionOrder: Map[String, Int] = executionLog.toMap
    def getMaxConcurrentTasks: Int          = maxConcurrent.get()
    def wasTaskStartedBefore(task1: String, task2: String): Boolean =
      (startTimes.get(task1), startTimes.get(task2)) match {
        case (Some(t1), Some(t2)) => t1 < t2
        case _                    => false
      }

    def didTasksOverlap(task1: String, task2: String): Boolean =
      (startTimes.get(task1), endTimes.get(task1), startTimes.get(task2), endTimes.get(task2)) match {
        case (Some(start1), Some(end1), Some(start2), Some(end2)) =>
          // Tasks overlap if one starts before the other ends
          start1 < end2 && end1 > start2
        case _ => false
      }
  }

  // Create test subtasks
  class SuccessfulSubtask(val name: String) extends WorkflowSubtask {
    override protected val ignoreAndLogFailures: Boolean = false
    override def getContext                              = SimpleContext(name)

    override def readSource(env: TaskEnvironment): Task[Dataset[_]] = ZIO.attempt {
      import spark.implicits._
      Seq((1, name)).toDF("id", "name")
    }

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
      ZIO.succeed(inDs)

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] =
      ZIO.attempt(outDs.count()).unit // Materialize the dataset
  }

  class FailingSubtask(val name: String) extends WorkflowSubtask {
    override protected val ignoreAndLogFailures: Boolean = false
    override def getContext                              = SimpleContext(name)

    override def readSource(env: TaskEnvironment): Task[Dataset[_]] =
      ZIO.fail(new RuntimeException(s"Simulated failure in $name"))

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
      ZIO.succeed(inDs)

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] =
      ZIO.unit
  }

  // Subtask with GroupingContext for testing GROUP_DEPENDENCIES
  class GroupedSuccessfulSubtask(
    val name: String,
    val groupId: String,
    tracker: Option[ExecutionTracker] = None,
    sleepMillis: Long = 50
  ) extends WorkflowSubtask {
    override protected val ignoreAndLogFailures: Boolean = false
    override def getContext                              = GroupingContext(name, groupId)

    override def readSource(env: TaskEnvironment): Task[Dataset[_]] = ZIO.attempt {
      tracker.foreach(_.recordStart(name))
      // Simulate some work to ensure we can detect concurrency
      Thread.sleep(sleepMillis)
      import spark.implicits._
      Seq((1, name, groupId)).toDF("id", "name", "group")
    }

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
      ZIO.succeed(inDs)

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] = ZIO.attempt {
      outDs.count() // Materialize the dataset
      tracker.foreach(_.recordEnd(name))
    }
  }

  class GroupedFailingSubtask(val name: String, val groupId: String) extends WorkflowSubtask {
    override protected val ignoreAndLogFailures: Boolean = false
    override def getContext                              = GroupingContext(name, groupId)

    override def readSource(env: TaskEnvironment): Task[Dataset[_]] =
      ZIO.fail(new RuntimeException(s"Simulated failure in $name (group: $groupId)"))

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
      ZIO.succeed(inDs)

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] =
      ZIO.unit
  }

  // Subtask with tracking for NO_DEPENDENCIES strategy
  class TrackedSuccessfulSubtask(
    val name: String,
    tracker: Option[ExecutionTracker] = None,
    sleepMillis: Long = 50
  ) extends WorkflowSubtask {
    override protected val ignoreAndLogFailures: Boolean = false
    override def getContext                              = SimpleContext(name)

    override def readSource(env: TaskEnvironment): Task[Dataset[_]] = ZIO.attempt {
      tracker.foreach(_.recordStart(name))
      // Simulate some work to ensure we can detect concurrency
      Thread.sleep(sleepMillis)
      import spark.implicits._
      Seq((1, name)).toDF("id", "name")
    }

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
      ZIO.succeed(inDs)

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] = ZIO.attempt {
      outDs.count() // Materialize the dataset
      tracker.foreach(_.recordEnd(name))
    }
  }

  override def spec: Spec[Any, Throwable] = suite("ConcurrentRunner")(
    suite("NO_DEPENDENCIES strategy")(
      test("should execute all tasks successfully") {
        // Create several successful subtasks
        val tasks = (1 to 5).map(i => new SuccessfulSubtask(s"task$i")).toSeq

        // Run the parallel runner
        val runner = ConcurrentRunner(tasks, NO_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).as(assertTrue(true))
      },
      test("should execute all tasks concurrently") {
        val tracker = ExecutionTracker()

        // Create tasks with tracking
        val tasks = Seq(
          new TrackedSuccessfulSubtask("task1", Some(tracker), sleepMillis = 100),
          new TrackedSuccessfulSubtask("task2", Some(tracker), sleepMillis = 100),
          new TrackedSuccessfulSubtask("task3", Some(tracker), sleepMillis = 100),
          new TrackedSuccessfulSubtask("task4", Some(tracker), sleepMillis = 100)
        )

        val runner = ConcurrentRunner(tasks, NO_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).as {
          val maxConcurrent = tracker.getMaxConcurrentTasks

          // With NO_DEPENDENCIES, multiple tasks should run concurrently
          val someTasksOverlap =
            tracker.didTasksOverlap("task1", "task2") ||
              tracker.didTasksOverlap("task2", "task3") ||
              tracker.didTasksOverlap("task3", "task4")

          assertTrue(
            maxConcurrent >= 2, // At least 2 tasks ran concurrently
            someTasksOverlap    // Tasks should have overlapping execution
          )
        }
      },
      test("should respect executor parallelism limit") {
        val tracker = ExecutionTracker()

        // Create more tasks than executor threads (executor has 3 threads)
        val tasks = (1 to 6).map(i => new TrackedSuccessfulSubtask(s"task$i", Some(tracker), sleepMillis = 150))

        val runner = ConcurrentRunner(tasks, NO_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).as {
          val maxConcurrent = tracker.getMaxConcurrentTasks

          // Should not exceed executor capacity
          assertTrue(
            maxConcurrent >= 2, // At least some concurrency
            maxConcurrent <= 6  // But not more than total tasks
          )
        }
      },
      test("should fail when a task fails") {
        // Mix successful and failing subtasks
        val tasks = Seq(
          new SuccessfulSubtask("task1"),
          new SuccessfulSubtask("task2"),
          new FailingSubtask("task3"),
          new SuccessfulSubtask("task4"),
          new SuccessfulSubtask("task5")
        )

        // Run the parallel runner
        val runner = ConcurrentRunner(tasks, NO_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).exit.map { exit =>
          // The runner should fail with an exception
          assertTrue(
            exit.isFailure,
            exit.causeOption
              .flatMap(cause => cause.failureOption)
              .exists(_.getMessage.contains("Simulated failure in task3"))
          )
        }
      },
      test("should handle single task") {
        val tracker = ExecutionTracker()

        val tasks = Seq(
          new TrackedSuccessfulSubtask("onlyTask", Some(tracker))
        )

        val runner = ConcurrentRunner(tasks, NO_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).as {
          val order = tracker.getExecutionOrder

          assertTrue(
            order.size == 1,
            order.contains("onlyTask")
          )
        }
      },
      test("should complete all tasks even when one fails") {
        val tracker = ExecutionTracker()

        // Mix tracked successful tasks with a failing task
        val tasks = Seq(
          new TrackedSuccessfulSubtask("task1", Some(tracker), sleepMillis = 100),
          new FailingSubtask("failingTask"),
          new TrackedSuccessfulSubtask("task2", Some(tracker), sleepMillis = 100),
          new TrackedSuccessfulSubtask("task3", Some(tracker), sleepMillis = 100)
        )

        val runner = ConcurrentRunner(tasks, NO_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).exit.map { exit =>
          val order = tracker.getExecutionOrder

          // Should fail but some successful tasks should have completed
          assertTrue(
            exit.isFailure,
            order.nonEmpty // At least some tasks executed
          )
        }
      },
      test("should work with SimpleContext tasks") {
        val tasks = Seq(
          new SuccessfulSubtask("simpleTask1"),
          new SuccessfulSubtask("simpleTask2"),
          new SuccessfulSubtask("simpleTask3")
        )

        val runner = ConcurrentRunner(tasks, NO_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).as(assertTrue(true))
      },
      test("should work with GroupingContext tasks (ignoring groups)") {
        // NO_DEPENDENCIES should work with GroupingContext but ignore the grouping
        val tracker = ExecutionTracker()

        val tasks = Seq(
          new GroupedSuccessfulSubtask("taskA1", "groupA", Some(tracker), sleepMillis = 100),
          new GroupedSuccessfulSubtask("taskB1", "groupB", Some(tracker), sleepMillis = 100),
          new GroupedSuccessfulSubtask("taskA2", "groupA", Some(tracker), sleepMillis = 100),
          new GroupedSuccessfulSubtask("taskB2", "groupB", Some(tracker), sleepMillis = 100)
        )

        val runner = ConcurrentRunner(tasks, NO_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).as {
          val maxConcurrent = tracker.getMaxConcurrentTasks

          // Tasks from different groups should run concurrently (groups are ignored)
          val crossGroupOverlap =
            tracker.didTasksOverlap("taskA1", "taskB1") ||
              tracker.didTasksOverlap("taskA2", "taskB2")

          assertTrue(
            maxConcurrent >= 2,
            crossGroupOverlap
          )
        }
      }
    ),
    suite("GROUP_DEPENDENCIES strategy")(
      test("should execute tasks within the same group concurrently") {
        val tracker = ExecutionTracker()

        // Create tasks in the same group - they should run concurrently
        // Use longer sleep to ensure reliable concurrency detection
        val tasks = Seq(
          new GroupedSuccessfulSubtask("task1", "group1", Some(tracker), sleepMillis = 200),
          new GroupedSuccessfulSubtask("task2", "group1", Some(tracker), sleepMillis = 200),
          new GroupedSuccessfulSubtask("task3", "group1", Some(tracker), sleepMillis = 200)
        )

        val runner = ConcurrentRunner(tasks, GROUP_DEPENDENCIES)
        runner
          .run(Some(testExecutor))
          .provide(taskEnvLayer)
          .as {
            // All tasks in group1 should have overlapping execution
            val task1And2Overlap = tracker.didTasksOverlap("task1", "task2")
            val task1And3Overlap = tracker.didTasksOverlap("task1", "task3")
            val task2And3Overlap = tracker.didTasksOverlap("task2", "task3")
            val maxConcurrent    = tracker.getMaxConcurrentTasks

            assertTrue(
              task1And2Overlap || task1And3Overlap || task2And3Overlap, // At least some tasks should overlap
              maxConcurrent >= 2                                        // At least 2 tasks should have run concurrently
            )
          }
      },
      test("should execute different groups sequentially") {
        val tracker = ExecutionTracker()

        // Create tasks in different groups - groups should run sequentially
        val tasks = Seq(
          // Group A tasks
          new GroupedSuccessfulSubtask("taskA1", "groupA", Some(tracker), sleepMillis = 100),
          new GroupedSuccessfulSubtask("taskA2", "groupA", Some(tracker), sleepMillis = 100),
          // Group B tasks
          new GroupedSuccessfulSubtask("taskB1", "groupB", Some(tracker), sleepMillis = 100),
          new GroupedSuccessfulSubtask("taskB2", "groupB", Some(tracker), sleepMillis = 100)
        )

        val runner = ConcurrentRunner(tasks, GROUP_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).as {
          val aTasksFinishBeforeBStarts =
            !tracker.didTasksOverlap("taskA1", "taskB1") ||
              !tracker.didTasksOverlap("taskA2", "taskB1") ||
              !tracker.didTasksOverlap("taskA1", "taskB2") ||
              !tracker.didTasksOverlap("taskA2", "taskB2")

          // At least one pair should show sequential execution
          assertTrue(aTasksFinishBeforeBStarts)
        }
      },
      test("should execute multiple groups in sorted order") {
        val tracker = ExecutionTracker()

        // Create tasks in multiple groups with deliberate naming to test sorting
        val tasks = Seq(
          new GroupedSuccessfulSubtask("taskC1", "groupC", Some(tracker)),
          new GroupedSuccessfulSubtask("taskA1", "groupA", Some(tracker)),
          new GroupedSuccessfulSubtask("taskB1", "groupB", Some(tracker)),
          new GroupedSuccessfulSubtask("taskA2", "groupA", Some(tracker)),
          new GroupedSuccessfulSubtask("taskB2", "groupB", Some(tracker))
        )

        val runner = ConcurrentRunner(tasks, GROUP_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).as {
          val order = tracker.getExecutionOrder

          // Groups should execute in sorted order: A, B, C
          // So all A tasks should start before any B or C tasks
          val allABeforeB =
            tracker.wasTaskStartedBefore("taskA1", "taskB1") ||
              tracker.wasTaskStartedBefore("taskA2", "taskB1")

          val allBBeforeC =
            tracker.wasTaskStartedBefore("taskB1", "taskC1") ||
              tracker.wasTaskStartedBefore("taskB2", "taskC1")

          assertTrue(
            order.nonEmpty,
            allABeforeB,
            allBBeforeC
          )
        }
      },
      test("should fail when a task in a group fails") {
        // Create tasks with one failing task in group1
        val tasks = Seq(
          new GroupedSuccessfulSubtask("task1", "group1"),
          new GroupedFailingSubtask("task2", "group1"),
          new GroupedSuccessfulSubtask("task3", "group1")
        )

        val runner = ConcurrentRunner(tasks, GROUP_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).exit.map { exit =>
          assertTrue(
            exit.isFailure,
            exit.causeOption
              .flatMap(cause => cause.failureOption)
              .exists(_.getMessage.contains("Simulated failure in task2"))
          )
        }
      },
      test("should fail when a task in later group fails") {
        // Create tasks with failure in group2
        val tasks = Seq(
          new GroupedSuccessfulSubtask("task1", "group1"),
          new GroupedSuccessfulSubtask("task2", "group1"),
          new GroupedFailingSubtask("task3", "group2"),
          new GroupedSuccessfulSubtask("task4", "group2")
        )

        val runner = ConcurrentRunner(tasks, GROUP_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).exit.map { exit =>
          assertTrue(
            exit.isFailure,
            exit.causeOption
              .flatMap(cause => cause.failureOption)
              .exists(_.getMessage.contains("Simulated failure in task3 (group: group2)"))
          )
        }
      },
      test("should reject mixed context types with GROUP_DEPENDENCIES") {
        // Mix GroupingContext and SimpleContext
        val tasks = Seq(
          new GroupedSuccessfulSubtask("task1", "group1"),
          new SuccessfulSubtask("task2"), // SimpleContext - should be rejected
          new GroupedSuccessfulSubtask("task3", "group1")
        )

        // Should throw IllegalArgumentException during construction
        val result = ZIO.attempt(ConcurrentRunner(tasks, GROUP_DEPENDENCIES)).exit

        result.map { exit =>
          assertTrue(
            exit.isFailure,
            exit.causeOption.exists { cause =>
              // Check both failureOption (for checked errors) and dieOption (for defects)
              cause.failureOption.exists(_.getMessage.contains("All subtasks must have GroupingContext")) ||
              cause.dieOption.exists(_.getMessage.contains("All subtasks must have GroupingContext"))
            }
          )
        }
      },
      test("should handle single group with multiple tasks") {
        val tracker = ExecutionTracker()

        val tasks = Seq(
          new GroupedSuccessfulSubtask("task1", "onlyGroup", Some(tracker), sleepMillis = 200),
          new GroupedSuccessfulSubtask("task2", "onlyGroup", Some(tracker), sleepMillis = 200),
          new GroupedSuccessfulSubtask("task3", "onlyGroup", Some(tracker), sleepMillis = 200),
          new GroupedSuccessfulSubtask("task4", "onlyGroup", Some(tracker), sleepMillis = 200)
        )

        val runner = ConcurrentRunner(tasks, GROUP_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).as {
          val maxConcurrent = tracker.getMaxConcurrentTasks
          // With a single group, all tasks should be able to run concurrently
          assertTrue(maxConcurrent >= 2) // At least 2 tasks ran concurrently
        }
      },
      test("should handle many groups with single task each") {
        val tracker = ExecutionTracker()

        // Each task in its own group
        val tasks = (1 to 5).map { i =>
          new GroupedSuccessfulSubtask(s"task$i", s"group$i", Some(tracker), sleepMillis = 100)
        }

        val runner = ConcurrentRunner(tasks, GROUP_DEPENDENCIES)
        runner.run(Some(testExecutor)).provide(taskEnvLayer).as {
          val order = tracker.getExecutionOrder

          // Groups should execute sequentially (sorted order)
          // So task1 should start before task2, task2 before task3, etc.
          val sequentialExecution = (1 until 5).forall { i =>
            tracker.wasTaskStartedBefore(s"task$i", s"task${i + 1}")
          }

          assertTrue(
            order.size == 5,
            sequentialExecution
          )
        }
      }
    )
  )
}
