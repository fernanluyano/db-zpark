package dev.fb.dbzpark
package logging

import logging.DefaultLoggers.Builder.defaultConfiguration

import zio.logging.consoleJsonLogger
import zio.{Config, ConfigProvider, LogLevel, ZLayer}

/**
 * Provides standardized logging configuration for ZIO-based tasks. Provides optional integration with Kafka for remote
 * log publication.
 */
object DefaultLoggers {

  class Builder private {
    private var jsonConsoleLogger: Option[ZLayer[Any, Config.Error, Unit]] = None
    private var kafkaLogger: Option[KafkaLogger]                           = None

    def withJsonConsole(): Builder = {
      this.jsonConsoleLogger = Option(consoleJsonLogger())
      this
    }

    def withKafkaLogger(kafkaLogger: KafkaLogger): Builder = {
      this.kafkaLogger = Option(kafkaLogger)
      this
    }

    def build: ZLayer[Any, Config.Error, Unit] = {
      val loggers = (jsonConsoleLogger, kafkaLogger) match {
        case (None, None)                   => throw new IllegalStateException("you must provide at least one default logger")
        case (Some(jLogger), None)          => jLogger
        case (None, Some(kLogger))          => zio.Runtime.addLogger(kLogger)
        case (Some(jLogger), Some(kLogger)) => jLogger ++ zio.Runtime.addLogger(kLogger)
      }

      zio.Runtime.removeDefaultLoggers >>> zio.Runtime.setConfigProvider(defaultConfiguration) >>> loggers
    }
  }

  object Builder {
    def apply(): Builder = new Builder

    /**
     * Provides the ZIO configuration for logging.
     *
     * The default implementation sets up a standardized log format and configures the root log level to Info. Override
     * this method to customize the log format or log levels for specific packages.
     *
     * [[https://zio.dev/zio-logging/formatting-log-records#log-format-configuration Configuration Docs]]
     * @return
     *   A ConfigProvider with logging configuration
     */
    private def defaultConfiguration: ConfigProvider = {
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
      ConfigProvider.fromMap(
        Map(
          "logger/format"           -> logFormat,
          "logger/filter/rootLevel" -> LogLevel.Info.label
        ),
        "/"
      )
    }
  }
}
