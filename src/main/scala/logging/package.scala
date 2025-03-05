package dev.fb.dbzpark

import org.apache.spark.sql.types._
import zio.json.{DeriveJsonEncoder, JsonEncoder, jsonField}

package object logging {
  implicit val logEntrySchema: JsonEncoder[DefaultLogEntry] = DeriveJsonEncoder.gen[DefaultLogEntry]

  /**
   * Represents a standardized log entry with fields for structured logging.
   *
   * This case class is designed to work with ZIO JSON for serialization and deserialization, with some fields using
   * custom JSON field names via @jsonField annotations.
   *
   * @param applicationName
   *   The name of the application generating the log entry (serialized as "app_name")
   * @param timestamp
   *   The timestamp when the log entry was created, as a formatted string
   * @param level
   *   The logging level (e.g., "INFO", "WARN", "ERROR")
   * @param thread
   *   The name of the thread that generated the log entry
   * @param message
   *   The main log message text
   * @param cause
   *   Optional exception or error information that caused this log entry
   * @param loggerName
   *   Optional name of the logger that created this entry (serialized as "logger_name")
   * @param spans
   *   Optional tracing spans associated with this log entry, as key-value pairs
   * @param customFields
   *   Optional additional fields that can be included with the log entry (serialized as "custom_fields")
   */
  case class DefaultLogEntry(
    @jsonField("app_name") applicationName: String,
    timestamp: String,
    level: String,
    thread: String,
    message: String,
    cause: Option[String],
    @jsonField("logger_name") loggerName: Option[String],
    spans: Option[Map[String, String]],
    @jsonField("custom_fields") customFields: Option[Map[String, String]]
  )

  /**
   * Creates and returns a Spark SQL schema that corresponds to the [[DefaultLogEntry]]. Useful if logs are sent to
   * Kafka, S3, or any other system you can read the logs from.
   *
   * @return
   *   A StructType representing the Spark SQL schema for log entries
   */
  def getDefaultLogEntrySparkSchema: StructType =
    StructType(
      Seq(
        StructField("app_name", StringType, nullable = false),
        StructField("timestamp", TimestampType, nullable = false),
        StructField("level", StringType, nullable = false),
        StructField("thread", StringType, nullable = false),
        StructField("message", StringType, nullable = false),
        StructField("cause", StringType, nullable = true),
        StructField("logger_name", StringType, nullable = true),
        StructField("spans", MapType(StringType, StringType), nullable = true),
        StructField("custom_fields", MapType(StringType, StringType), nullable = true)
      )
    )

  case class KafkaLoggerContext(topic: String, appName: String)
}
