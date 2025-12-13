package dev.fb.dbzpark

package object logging {

  /**
   * Represents a log entry with all its fields.
   *
   * @param app_name The name of the application
   * @param timestamp The timestamp of the log entry
   * @param level The log level (e.g., INFO, WARN, ERROR)
   * @param thread The thread name
   * @param message The log message
   * @param cause The cause/exception if any
   * @param logger_name The name of the logger
   * @param spans Tracing spans information
   * @param custom_fields Additional custom fields
   */
  case class LogEntry(
    app_name: Option[String] = None,
    timestamp: Option[java.sql.Timestamp] = None,
    level: Option[String] = None,
    thread: Option[String] = None,
    message: Option[String] = None,
    cause: Option[String] = None,
    logger_name: Option[String] = None,
    spans: Option[Map[String, String]] = None,
    custom_fields: Option[Map[String, String]] = None
  )
}
