package examples

import logging.{DefaultLoggers, KafkaLogger, KafkaLoggerContext}

import dev.fb.dbzpark.{TaskEnvironment, WorkflowTask}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import zio._

import java.util.Properties

/**
 * A simple example demonstrating the basic usage of WorkflowTask.
 * This workflow reads a CSV file, performs a simple transformation, and writes the results.
 */
object BasicWorkflowTaskExample extends WorkflowTask {

  /**
   * Define our custom task environment that holds the SparkSession and application name.
   */
  private class SimpleTaskEnvironment(val sparkSession: SparkSession, val appName: String) extends TaskEnvironment

  /**
   * Build the task environment with a SparkSession configured for our needs.
   */
  override protected def buildTaskEnvironment: TaskEnvironment = {
    // Create a SparkSession - In a real Databricks environment,
    // you would typically use SparkSession.builder().getOrCreate()
    val spark = SparkSession
      .builder()
      .appName("Basic Workflow Example")
      .master("local[*]") // Use local mode for this example
      .getOrCreate()

    new SimpleTaskEnvironment(spark, "Basic Workflow Example")
  }

  /**
   * Define the main task logic using ZIO.
   * Do this when you want to fully control the flow and have ZIO knowledge.
   * See the other examples.
   */
  override protected def startTask: ZIO[TaskEnvironment, Throwable, Unit] =
    for {
      env           <- ZIO.service[TaskEnvironment]
      _             <- ZIO.logInfo("Starting data processing")
      inputData     <- ZIO.attempt(readCsv(env))
      processedData <- processData(inputData)
      _             <- ZIO.attempt(write(processedData))
      _             <- ZIO.logInfo("Data processing completed successfully")
    } yield ()

  private def readCsv(env: TaskEnvironment) =
    env.sparkSession.read
      .option("header", "true")
      .csv("data/input/sample.csv")

  private def processData(df: DataFrame): Task[DataFrame] =
    ZIO.attempt {
      // Simple transformation: filter and add a column
      val filtered = df.filter("age > 18")
      filtered.withColumn("processing_date", org.apache.spark.sql.functions.current_date())
    }

  private def write(processedData: DataFrame): Unit =
    processedData.write
      .mode("overwrite")
      .parquet("data/output/processed_data")

  /**
   * Optionally, override the bootstrap layer to configure logging.
   * This sets up both console JSON logging and Kafka logging.
   * The [[DefaultLoggers]] object contains a utility builder with support for
   * a default consoler logger and a kafka logger.
   *
   * For more info about configuring logging for ZIO
   * go to https://zio.dev/reference/observability/logging/
   */
  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    DefaultLoggers
      .Builder()
      .withJsonConsole()
      .withKafkaLogger(createKafkaLogger)
      .build

  /**
   * Create a Kafka logger for remote log aggregation.
   */
  private def createKafkaLogger: KafkaLogger = {
    // In a real environment, these would come from configuration
    val kafkaServers = getKafkaServers

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // Add security settings for AWS MSK (if needed)
    if (useAwsMsk) {
      props.put("security.protocol", "SASL_SSL")
      props.put("sasl.mechanism", "AWS_MSK_IAM")
      props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
      props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
    }

    val producer = new KafkaProducer[String, String](props)
    val context = KafkaLoggerContext(
      topic = "spark-logs",
      appName = "Workflow With Logging"
    )

    KafkaLogger(producer, context)
  }

  // Helper methods to get configuration
  private def getKafkaServers: String =
    // In a real app, get from configuration or environment
    sys.env.getOrElse("KAFKA_SERVERS", "localhost:9092")

  private def useAwsMsk: Boolean =
    // In a real app, determine from configuration or environment
    sys.env.get("USE_AWS_MSK").exists(_.toBoolean)
}
