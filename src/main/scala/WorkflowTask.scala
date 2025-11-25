package dev.fb.dbzpark

import subtask.ExecutionModel

import zio.logging.LogAnnotation
import zio.{Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault, ZLayer, durationLong}

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

  protected def getExecutionModel(env: TaskEnvironment): ExecutionModel

  /**
   * Runs the workflow task, initializing the environment and executing the task.
   */
  override def run: ZIO[Any with ZIOAppArgs with Scope, Throwable, Unit] = {
    def _run(environment: TaskEnvironment) =
      for {
        startNanos <- ZIO.succeed(System.nanoTime())
        _          <- ZIO.logInfo(s"Starting task: ${environment.appName}")
        execModel  <- ZIO.attempt(getExecutionModel(environment))
        _ <- execModel.run
               .provide(ZLayer.succeed(environment))
               .foldZIO(
                 success = _ => happyPath(environment, startNanos),
                 failure = e => sadPath(environment, startNanos, e)
               )
      } yield ()

    ZIO
      .attempt(buildTaskEnvironment)
      .foldZIO(
        success = e => _run(e) @@ appNameAnnotation(e.appName),
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
