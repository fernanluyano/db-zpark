# db-zpark

[![Scala CI](https://github.com/fernanluyano/db-zpark/actions/workflows/build.yml/badge.svg)](https://github.com/fernanluyano/db-zpark/actions/workflows/build.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.fernanluyano/db-zpark_2.12.svg)](https://central.sonatype.com/artifact/io.github.fernanluyano/db-zpark_2.12)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A code-first approach to manage Spark/Scala jobs, built on the ZIO framework and geared for Databricks environments.

## Overview

db-zpark provides a structured, functional programming approach to developing Spark applications in Databricks.
By leveraging ZIO's powerful effect system, this library helps you build robust, testable, and maintainable data pipelines.

Key features include:
- Structured workflow task architecture using ZIO
- Automatic timing and metrics for task execution
- Comprehensive logging framework with Kafka integration
- Built-in error handling and reporting
- JSON-based structured logging

## Databricks Runtime Compatibility
See [Databricks Runtime releases](https://docs.databricks.com/aws/en/release-notes/runtime/#supported-databricks-runtime-lts-releases)

| db-zpark Version | Databricks Runtime | Spark Version | Delta Lake Version | Scala Version | JDK Version | ZIO Version |
|------------------|-------------------|---------------|--------------------|---------------|------------|-------------|
| 0.1.x            | 15.4 LTS          | 3.5.4         | 3.2.1              | 2.12.18       | 17         | 2.x         |

## Installation

Add db-zpark to your SBT project:

```scala
libraryDependencies += "io.github.fernanluyano" %% "db-zpark" % "x.y.z"
```

Where `x.y.z` is the latest version available on [Maven Central](https://central.sonatype.com/artifact/io.github.fernanluyano/db-zpark_2.12).

## Usage

### Creating a Spark Workflow Task

The core of db-zpark is the `WorkflowTask` trait, which provides a structured way to create Spark jobs with built-in logging and error handling:

```scala
import dev.fb.dbzpark.{TaskEnvironment, WorkflowTask}
import org.apache.spark.sql.{DataFrame, SparkSession}
import zio._

// 1. Define your task environment
class MyTaskEnvironment(val sparkSession: SparkSession, val appName: String) extends TaskEnvironment

// 2. Create your workflow task
object MySparkJob extends WorkflowTask {

  // Build the task environment with Spark session
  override protected def buildTaskEnvironment: TaskEnvironment = {
    val spark = SparkSession.builder()
      .appName("My Spark Job")
      .master("local[*]")
      .getOrCreate()

    new MyTaskEnvironment(spark, "My Spark Job")
  }

  // Define your task logic
  override protected def startTask: ZIO[TaskEnvironment, Throwable, Unit] = {
    for {
      env <- ZIO.service[TaskEnvironment]
      _   <- ZIO.logInfo("Starting data processing")
      // Use the provided Spark session
      data <- ZIO.attempt(env.sparkSession.read.csv("path/to/data.csv"))
      // Process your data
      result <- processData(data)

      // Save results
      _ <- ZIO.attempt(result.write.parquet("path/to/output"))
      _ <- ZIO.logInfo("Data processing completed")
    } yield ()
  }

  private def processData(df: DataFrame): Task[DataFrame] = {
    ZIO.attempt {
      // Your data transformation logic
      df.filter("column > 10")
    }
  }
}
```

### Configuring Logging (optional)

db-zpark provides a flexible logging system with console and Kafka options. This example creates a default logger
that sends the output in json format to the console and also to a kafka topic in MSK (assuming your config is correct).
To do so just override the `bootstrap` val. If not it will just use the default console logger only.


```scala
import logging.{DefaultLoggers, KafkaLogger, KafkaLoggerContext}

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.sql.SparkSession
import zio.{ZIO, ZIOAppArgs, ZLayer}

object MySparkJobWithKafkaLogging extends WorkflowTask {
  class MyTaskEnvironment(val sparkSession: SparkSession, val appName: String) extends TaskEnvironment

  override protected def buildTaskEnvironment: TaskEnvironment = {
    val spark = SparkSession.builder()
      .appName("My Spark Job")
      .master("local[*]")
      .getOrCreate()

    new MyTaskEnvironment(spark, "My Spark Job")
  }

  // define the spark logic, see example above
  override protected def startTask: ZIO[TaskEnvironment, Throwable, Unit] = ZIO.unit

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] = DefaultLoggers
    .Builder()
    .withJsonConsole()
    .withKafkaLogger(createKafkaLogger)
    .build

  private def createKafkaLogger: KafkaLogger = {
    val servers = sys.env("KAFKA_SERVERS")
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put("security.protocol", "SASL_SSL")
    props.put("sasl.mechanism", "AWS_MSK_IAM")
    props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
    props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")

    val producer = new KafkaProducer[String, String](props)
    val context = KafkaLoggerContext(topic = "spark-logs", appName = "My Spark Job")

    KafkaLogger(producer, context)
  }
}
```

## Building

This project uses a Makefile to simplify build commands:

```bash
# Full build (clean, format check, compile, test)
make build

# Format code
make format

# See all available commands
make help
```

## CI/CD

This project uses GitHub Actions for continuous integration and delivery:

- CI runs on pushes to `develop`, `master`, and all `release/*` branches
- CI also runs on all pull requests targeting these branches
- The workflow checks code formatting, compiles the project, and runs tests
- Dependency information is submitted to GitHub for security vulnerability alerts

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -am 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.