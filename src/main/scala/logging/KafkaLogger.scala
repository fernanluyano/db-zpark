package dev.fb.dbzpark
package logging

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import zio.json.EncoderOps
import zio.{Cause, FiberId, FiberRefs, LogLevel, LogSpan, Trace, ZLogger}

import java.time.{LocalDateTime, ZoneId}

/**
 * A custom ZIO logger implementation that publishes log entries to a Kafka topic.
 *
 * This logger serializes log entries as JSON and sends them to a specified Kafka topic using a predefined message
 * format with [[DefaultLogEntry]].
 *
 * @param kafkaProducer
 *   Kafka producer instance for sending string messages.
 * @param kafkaContext
 *   Destination Kafka topic for log messages and application identifier (the spark application name).
 */
class KafkaLogger private (
  private val kafkaProducer: KafkaProducer[String, String],
  private val kafkaContext: KafkaLoggerContext
) extends ZLogger[String, Unit] {

  override def apply(
    trace: Trace,
    fiberId: FiberId,
    logLevel: LogLevel,
    message: () => String,
    cause: Cause[Any],
    context: FiberRefs,
    spans: List[LogSpan],
    annotations: Map[String, String]
  ): Unit = {
    val _cause = Option(cause.prettyPrint).filter(_.nonEmpty)
    val logEntry = DefaultLogEntry(
      applicationName = kafkaContext.appName,
      timestamp = LocalDateTime.now(ZoneId.of("UTC")).toString,
      level = logLevel.label,
      thread = fiberId.threadName,
      message = message(),
      cause = _cause,
      loggerName = extractLoggerName(trace),
      spans = getAsMapOrNone(getSpans(spans)),
      customFields = getAsMapOrNone(annotations.toList)
    ).toJson

    val record = new ProducerRecord[String, String](kafkaContext.topic, logEntry)
    kafkaProducer.send(record)
  }

  private def extractLoggerName(trace: Trace): Option[String] =
    trace match {
      case Trace(location, _, _) =>
        val top = location.lastIndexOf(".")
        val name = if (top > 0) {
          location.substring(0, top)
        } else {
          location
        }
        Some(name)
      case _ => None
    }

  private def getSpans(span: List[LogSpan]): Seq[(String, String)] =
    span.map { sp =>
      val _time = System.currentTimeMillis() - sp.startTime
      sp.label -> s"${_time}ms"
    }

  /**
   * To avoid serializing empty maps.
   */
  private def getAsMapOrNone(values: Seq[(String, String)]): Option[Map[String, String]] =
    values match {
      case Nil => None
      case l   => Some(l.toMap)
    }
}

object KafkaLogger {

  def apply(kafkaProducer: KafkaProducer[String, String], context: KafkaLoggerContext): KafkaLogger =
    new KafkaLogger(kafkaProducer, context)
}
