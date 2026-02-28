package dev.fb.dbzpark

import subtask._

import org.apache.spark.sql.{Dataset, SparkSession}
import zio._
import zio.test._

import java.util.concurrent.Executors

object WorkflowTaskSpec extends ZIOSpecDefault {
  private val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("test-app")
    .getOrCreate()

  class TestTaskEnvironment(graph: SubtasksGraph, failFastFlag: Boolean = false) extends TaskEnvironment {
    override def sparkSession: SparkSession   = spark
    override def appName: String              = "test-app"
    override def subtasksGraph: SubtasksGraph = graph
    override def subtasksExecutor: Executor   = Executor.fromJavaExecutor(Executors.newFixedThreadPool(4))
    override def maxConcurrentSubtasks: Int   = 4
    override def failFast: Boolean            = failFastFlag
  }

  val appArgsLayer: ZLayer[Any, Nothing, ZIOAppArgs] = ZIOAppArgs.empty

  override def spec = suite("WorkflowTask")(
    test("successful task") {
      val subtask = new WorkflowSubtask {
        override val taskId: String = "test-subtask"

        override def readSource(env: TaskEnvironment): Task[Dataset[_]] =
          ZIO.attempt(env.sparkSession.sql("select 1 as n"))

        override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] = ZIO.attempt(inDs)

        override def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] =
          ZIO.attempt(outDs.count()).unit
      }

      val graph = SubtasksGraph.Builder().addSubtask(subtask).build

      val myTask = new WorkflowTask {
        override protected def buildTaskEnvironment = new TestTaskEnvironment(graph)
      }

      for {
        _            <- myTask.run.provideSomeLayer[Scope](appArgsLayer)
        loggerOutput <- ZTestLogger.logOutput
        messages     <- ZIO.attempt(loggerOutput.map(_.message()).toSet)
      } yield assertTrue(messages.exists(_.startsWith("Task test-app finished successfully")))
    },
    test("failing task") {
      val subtask = new WorkflowSubtask {
        override val taskId: String = "failing-subtask"

        override def readSource(env: TaskEnvironment): Task[Dataset[_]] =
          ZIO.fail(new RuntimeException("Task failed intentionally"))

        override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] = ZIO.attempt(inDs)

        override def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] = ZIO.unit
      }

      val graph = SubtasksGraph.Builder().addSubtask(subtask).build

      val failingTask = new WorkflowTask {
        override protected def buildTaskEnvironment = new TestTaskEnvironment(graph, failFastFlag = true)
      }

      for {
        _            <- failingTask.run.provideSomeLayer[Scope](appArgsLayer).exit
        loggerOutput <- ZTestLogger.logOutput
        messages     <- ZIO.attempt(loggerOutput.map(_.message()).toSet)
      } yield assertTrue(
        messages.exists(m => m.contains("Task test-app failed") && m.contains("Task failed intentionally"))
      )
    },
    test("environment build failure") {
      val badEnvTask = new WorkflowTask {
        override protected def buildTaskEnvironment =
          throw new RuntimeException("Failed to build environment")
      }

      for {
        exit <- badEnvTask.run.provideSomeLayer[Scope](appArgsLayer).exit
      } yield assertTrue(exit.isFailure)
    }
  )
}
