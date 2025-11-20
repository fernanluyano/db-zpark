package dev.fb.dbzpark

import subtask.ExecutionModel

import zio.logging.LogAnnotation
import zio.{Scope, Task, UIO, ZIO, ZIOAppArgs, ZIOAppDefault, ZLayer, durationLong}

/**
 * The interface for defining a Databricks workflow task using ZIO. Handles environment setup, execution, and error
 * management.
 */
trait WorkflowTask extends ZIOAppDefault {
  private val appNameAnnotation =
    LogAnnotation[String](
      name = "appName",
      combine = (_, a) => a,
      render = identity
    )

  protected def getExecutionModel: ExecutionModel

  /**
   * Runs the workflow task, initializing the environment and executing the task.
   */
  override def run: ZIO[Any with ZIOAppArgs with Scope, Throwable, Unit] = {
    def _run(environment: TaskEnvironment) =
      for {
        startNanos <- ZIO.succeed(System.nanoTime())
        _          <- ZIO.logInfo(s"Starting task: ${environment.appName}")
        execModel  <- ZIO.attempt(getExecutionModel)
        _ <- execModel.run
               .provide(ZLayer.succeed(environment))
               .foldZIO(
                 success = _ => happyPath(startNanos),
                 failure = e => sadPath(startNanos, e)
               )
      } yield ()

    ZIO
      .attempt(buildTaskEnvironment)
      .foldZIO(
        success = e => _run(e) @@ appNameAnnotation(e.appName),
        failure = e => ZIO.logError(e.getMessage) *> ZIO.fail(e)
      )
  }

  /**
   * Builds the task execution environment: external dependencies, basic info about the task, spark session etc.
   */
  protected def buildTaskEnvironment: TaskEnvironment

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
