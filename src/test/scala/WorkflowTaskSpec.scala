package dev.fb.dbzpark

import logging.DefaultLoggers
import subtask._

import org.apache.spark.sql.{Dataset, SparkSession}
import zio._
import zio.test._

object WorkflowTaskSpec extends ZIOSpecDefault {
  private val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("test-app")
    .getOrCreate()

  class TestTaskEnvironment extends TaskEnvironment {
    override def sparkSession: SparkSession = spark
    override def appName: String            = "test-app"
  }

  // Create a layer providing ZIOAppArgs
  val appArgsLayer: ZLayer[Any, Nothing, ZIOAppArgs] = ZIOAppArgs.empty

  // Create a logging layer using the builder
  val loggingLayer: ZLayer[Any, Config.Error, Unit] = DefaultLoggers
    .Builder()
    .withJsonConsole()
    .build

  override def spec = suite("WorkflowTask")(
    test("successful task") {
      val myTask = new WorkflowTask {
        override protected def buildTaskEnvironment = new TestTaskEnvironment

        override protected def getExecutionModel: ExecutionModel = {
          val subtask = new WorkflowSubtask {
            override protected val ignoreAndLogFailures: Boolean = false
            override def getContext                              = SimpleContext("test-subtask")

            override def readSource(env: TaskEnvironment): Dataset[_] =
              env.sparkSession.sql("select 1 as n")

            override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] = inDs

            override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit =
              outDs.count() // Materialize the dataset
          }
          ExecutionModel.singleton(subtask)
        }

        // Override the bootstrap to use our test logging layer
        override val bootstrap = loggingLayer
      }

      for {
        t            <- ZIO.succeed(myTask)
        _            <- t.run.provideSomeLayer[Scope](appArgsLayer)
        loggerOutput <- ZTestLogger.logOutput
        messages     <- ZIO.attempt(loggerOutput.map(_.message()).toSet)
      } yield assertTrue(messages.exists(e => e.startsWith("Task test-app finished successfully")))
    },
    test("failing task") {
      val failingTask = new WorkflowTask {
        override protected def buildTaskEnvironment = new TestTaskEnvironment

        override protected def getExecutionModel: ExecutionModel = {
          val subtask = new WorkflowSubtask {
            override protected val ignoreAndLogFailures: Boolean = false
            override def getContext                              = SimpleContext("failing-subtask")

            override def readSource(env: TaskEnvironment): Dataset[_] =
              throw new RuntimeException("Task failed intentionally")

            override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] = inDs

            override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit = {}
          }
          ExecutionModel.singleton(subtask)
        }

        // Override the bootstrap to use our test logging layer
        override val bootstrap = loggingLayer
      }

      for {
        t            <- ZIO.succeed(failingTask)
        _            <- t.run.provideSomeLayer[Scope](appArgsLayer).exit
        loggerOutput <- ZTestLogger.logOutput
        messages     <- ZIO.attempt(loggerOutput.map(_.message()).toSet)
      } yield assertTrue(messages.contains("Task test-app failed in 0 seconds due to: Task failed intentionally"))
    },
    test("environment build failure") {
      val badEnvTask = new WorkflowTask {
        override protected def buildTaskEnvironment =
          throw new RuntimeException("Failed to build environment")

        override protected def getExecutionModel: ExecutionModel = {
          val subtask = new WorkflowSubtask {
            override protected val ignoreAndLogFailures: Boolean = false
            override def getContext                              = SimpleContext("test-subtask")

            override def readSource(env: TaskEnvironment): Dataset[_] =
              env.sparkSession.sql("select 1 as n")

            override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] = inDs

            override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit = {}
          }
          ExecutionModel.singleton(subtask)
        }

        // Override the bootstrap to use our test logging layer
        override val bootstrap = loggingLayer
      }

      for {
        t    <- ZIO.succeed(badEnvTask)
        exit <- t.run.provideSomeLayer[Scope](appArgsLayer).exit
      } yield assertTrue(exit.isFailure)
    }
  )
}
