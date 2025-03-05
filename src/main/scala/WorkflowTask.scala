package dev.fb.dbzpark

import zio.{Scope, Task, UIO, ZIO, ZIOAppArgs, ZIOAppDefault, ZLayer, durationLong}

/**
 * The interface for defining a Databricks workflow task using ZIO. Handles environment setup, execution, and error
 * management.
 */
trait WorkflowTask extends ZIOAppDefault {

  /**
   * Runs the workflow task, initializing the environment and executing the task.
   */
  override def run: ZIO[Any with ZIOAppArgs with Scope, Throwable, Unit] =
    for {
      startNanos  <- ZIO.succeed(System.nanoTime())
      environment <- ZIO.attempt(buildTaskEnvironment)
      _           <- ZIO.logInfo(s"Starting task: ${environment.appName}")
      _ <- startTask
             .provide(ZLayer.succeed(environment))
             .foldZIO(
               success = _ => happyPath(startNanos),
               failure = e => sadPath(startNanos, e)
             )
    } yield ()

  /**
   * Builds the task execution environment: external dependencies, basic info about the task, spark session etc.
   */
  protected def buildTaskEnvironment: TaskEnvironment

  /**
   * Defines the main logic of the task.
   */
  protected def startTask: ZIO[TaskEnvironment, Throwable, Unit]

  /**
   * Terminates successfully.
   */
  private def happyPath(startTimeNanos: Long): UIO[Unit] = {
    val elapsedSeconds = (System.nanoTime() - startTimeNanos).nanos.toSeconds
    ZIO.logInfo(s"Task ${buildTaskEnvironment.appName} finished successfully in $elapsedSeconds seconds")
  }

  /**
   * Terminate the task with failure.
   */
  private def sadPath(startTimeNanos: Long, cause: Throwable): Task[Unit] = {
    val elapsedSeconds = (System.nanoTime() - startTimeNanos).nanos.toSeconds
    val message        = s"Task ${buildTaskEnvironment.appName} failed in $elapsedSeconds seconds due to: ${cause.getMessage}"
    ZIO.logInfo(message) *> ZIO.fail(cause)
  }
}
