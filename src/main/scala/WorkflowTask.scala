package dev.fb.dbzpark

import subtask.SubtasksManager

import zio.logging.LogAnnotation
import zio.{Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault, durationLong}
import zio.ZLayer

/**
 * The interface for defining a Databricks workflow task using ZIO. Handles environment setup, execution, and error
 * management.
 */
trait WorkflowTask extends ZIOAppDefault {
  private val appNameAnnotation =
    LogAnnotation[String](
      name = "app_name",
      combine = (_, a) => a,
      render = identity
    )

  /**
   * Runs the workflow task, initializing the environment and executing the task.
   */
  override def run: ZIO[Any with ZIOAppArgs with Scope, Throwable, Unit] = {
    def _run =
      for {
        environment <- ZIO.service[TaskEnvironment]
        startNanos  <- ZIO.succeed(System.nanoTime())
        _           <- ZIO.logInfo(s"Starting task: ${environment.appName}")
        manager     <- ZIO.attempt(SubtasksManager(environment))
        _ <- manager.run.foldZIO(
               success = _ => happyPath(environment, startNanos),
               failure = e => sadPath(environment, startNanos, e)
             )
      } yield ()

    ZIO
      .attempt(buildTaskEnvironment)
      .foldZIO(
        success = e => _run.provide(ZLayer.fromZIO(ZIO.attempt(e))) @@ appNameAnnotation(e.appName),
        failure = e => ZIO.logError(e.getMessage) *> ZIO.fail(e)
      )
  }

  protected def buildTaskEnvironment: TaskEnvironment

  /**
   * Hook method called after task execution (success or failure).
   *
   * Override this method to perform cleanup, log persistence, or other
   * finalization tasks. The default implementation does nothing.
   *
   * @param env The task environment
   * @return A Task that completes when post-processing is done
   */
  protected def finalizeTask(env: TaskEnvironment): Task[Unit] = ZIO.attempt(env).unit

  private def happyPath(env: TaskEnvironment, startTimeNanos: Long): Task[Unit] = {
    true match {
      case true => ()
    }
    val elapsedSeconds = (System.nanoTime() - startTimeNanos).nanos.toSeconds
    ZIO.logInfo(s"Task ${env.appName} finished successfully in $elapsedSeconds seconds") *> finalizeTask(env)
  }

  private def sadPath(env: TaskEnvironment, startTimeNanos: Long, cause: Throwable): Task[Unit] = {
    val elapsedSeconds = (System.nanoTime() - startTimeNanos).nanos.toSeconds
    val message        = s"Task ${env.appName} failed in $elapsedSeconds seconds due to: ${cause.getMessage}"
    ZIO.logError(message) *>
      finalizeTask(env) *>
      ZIO.fail(cause)
  }
}
