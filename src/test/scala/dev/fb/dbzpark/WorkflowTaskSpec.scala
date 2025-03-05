package dev.fb.dbzpark

import logging.DefaultLoggers

import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.mock
import zio._
import zio.test._

object WorkflowTaskSpec extends ZIOSpecDefault {
  class TestTaskEnvironment extends TaskEnvironment {
    override def sparkSession: SparkSession = mock(classOf[SparkSession])
    override def environmentName: String = "test"
    override def appName: String = "test-app"
  }

  // Create a layer providing ZIOAppArgs
  val appArgsLayer: ZLayer[Any, Nothing, ZIOAppArgs] = ZIOAppArgs.empty

  // Create a logging layer using the builder
  val loggingLayer: ZLayer[Any, Config.Error, Unit] = DefaultLoggers.Builder()
    .withJsonConsole()
    .build

  override def spec = suite("WorkflowTask")(
    test("successful task") {
      val myTask = new WorkflowTask {
        override protected def buildTaskEnvironment = new TestTaskEnvironment
        override protected def startTask: ZIO[TaskEnvironment, Throwable, Unit] = ZIO.unit
        // Override the bootstrap to use our test logging layer
        override val bootstrap = loggingLayer
      }

      for {
        t            <- ZIO.succeed(myTask)
        _            <- t.run.provideSomeLayer[Scope](appArgsLayer)
        loggerOutput <- ZTestLogger.logOutput
        messages     <- ZIO.attempt(loggerOutput.map(_.message()).toSet)
      } yield assertTrue(messages.contains("Task test-app finished successfully in 0 seconds"))
    },

    test("failing task") {
      val failingTask = new WorkflowTask {
        override protected def buildTaskEnvironment = new TestTaskEnvironment
        override protected def startTask: ZIO[TaskEnvironment, Throwable, Unit] = {
          ZIO.fail(new RuntimeException("Task failed intentionally"))
        }
        // Override the bootstrap to use our test logging layer
        override val bootstrap = loggingLayer
      }

      for {
        t <- ZIO.succeed(failingTask)
        _ <- t.run.provideSomeLayer[Scope](appArgsLayer).exit
        loggerOutput <- ZTestLogger.logOutput
        messages <- ZIO.attempt(loggerOutput.map(_.message()).toSet)
      } yield assertTrue(messages.contains("Task test-app failed in 0 seconds due to: Task failed intentionally"))
    },

    test("environment build failure") {
      val badEnvTask = new WorkflowTask {
        override protected def buildTaskEnvironment = {
          throw new RuntimeException("Failed to build environment")
        }
        override protected def startTask: ZIO[TaskEnvironment, Throwable, Unit] = ZIO.unit
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