package dev.fb.dbzpark
package logging

import unitycatalog.Tables.UcTable

import org.apache.spark.sql.SaveMode
import zio.logging.{consoleLogger, fileJsonLogger}
import zio.{ConfigProvider, LogLevel, Task, ZIO, ZIOAppArgs, ZLayer}

/**
 * Trait providing standardized logging capabilities for WorkflowTask implementations.
 *
 * This trait configures both console and file-based JSON logging, and optionally persists
 * logs to a Delta table for long-term storage and analysis.
 */
trait DefaultLogging { self: WorkflowTask =>

  /** Path where log files are written. Can be overridden for custom locations. */
  protected def logFilePath: String = "file:///tmp/db_zpark_logs.log"

  /**
   * Provides default loggers: console and json files.
   *
   * The file logger uses a time-based rolling policy and includes:
   *   - timestamp: ISO 8601 formatted timestamp
   *   - level: Log level (INFO, WARN, ERROR, etc.)
   *   - thread: Fiber ID
   *   - message: Log message text
   *   - cause: Optional exception information
   *   - logger_name: Source location of the log call
   *   - spans: Optional tracing spans
   *   - custom_fields: Additional key-value pairs (includes app_name)
   */
  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] = {
    val logFormat = Seq(
      "%label{timestamp}{%timestamp{yyyy-MM-dd'T'HH:mm:ssZ}}",
      "%label{level}{%level}",
      "%label{thread}{%fiberId}",
      "%label{message}{%message}",
      "%label{cause}{%cause}",
      "%label{logger_name}{%name}",
      "%label{spans}{%spans}",
      "%label{custom_fields}{%kvs}"
    ).mkString(" ")

    val config = ConfigProvider.fromMap(
      Map(
        "logger/format"           -> logFormat,
        "logger/filter/rootLevel" -> LogLevel.Info.label,
        "logger/path"             -> logFilePath
      ),
      "/"
    )

    val combinedLoggers = consoleLogger() ++ fileJsonLogger()

    zio.Runtime.removeDefaultLoggers >>>
      zio.Runtime.setConfigProvider(config) >>>
      combinedLoggers
  }

  /** Optional target table for log persistence */
  val logsTable: Option[UcTable]

  /**
   * Writes logs from the temporary file to the target Delta table.
   *
   * Override this method to customize how logs are written to Delta tables.
   * By default, it reads JSON logs, normalizes the app_name field, and appends
   * to the target table.
   */
  protected def deltaWriter(target: UcTable, env: TaskEnvironment): Task[Unit] = {

    val logs = ZIO.attempt {
      val _spark = env.sparkSession
      import _spark.implicits._

      env.sparkSession.read
        .format("json")
        .load(logFilePath)
        .as[LogEntry]
        .map { entry =>
          entry.copy(
            app_name = entry.app_name
              .orElse(entry.custom_fields.flatMap(_.get("app_name")))
              .orElse(Some("unknown"))
          )
        }
    }

    ZIO.logInfo("writing logs") *> logs.flatMap { df =>
      ZIO.attempt {
        df.write
          .format("delta")
          .mode(SaveMode.Append)
          .clusterBy("timestamp")
          .saveAsTable(target.getFullyQualifiedName)
      }
    }
  }

  /**
   * Persists logs to the configured Delta table.
   * If no target table is configured, this operation is skipped.
   */
  private def writeLogs(env: TaskEnvironment): Task[Unit] =
    logsTable match {
      case None => ZIO.logInfo("skipping writing logs")
      case Some(target) =>
        ZIO.logInfo(s"persisting logs to table ${target.getFullyQualifiedName}") *>
          deltaWriter(target, env)
    }

  /**
   * Override finalizeTask to automatically persist logs after execution.
   */
  override protected def finalizeTask(env: TaskEnvironment): Task[Unit] = writeLogs(env)
}
