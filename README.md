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
- Dependency injection (DI) with ZIO's ZLayer
- Automatic timing and metrics for task execution
- Comprehensive logging framework with Kafka integration
- Built-in error handling and reporting
- JSON-based structured logging
- Composable subtasks for complex workflows
- Sequential and parallel task execution

## Databricks Runtime Compatibility
See [Databricks Runtime releases](https://docs.databricks.com/aws/en/release-notes/runtime/#supported-databricks-runtime-lts-releases)

**Versioning Strategy**: db-zpark's major version follows the Databricks Runtime major version (e.g., db-zpark 2.x.x is compatible with Databricks Runtime 17.x LTS).

| db-zpark Version | Databricks Runtime | Spark Version | Delta Lake Version | Scala Version | JDK Version | ZIO Version |
|------------------|-------------------|---------------|--------------------|---------------|------------|-------------|
| 0.1.x            | 15.4 LTS          | 3.5.x         | 3.2.x              | 2.12.x        | 17         | 2.x         |
| 1.x.x            | 16.4 LTS          | 3.5.x         | 3.3.x              | 2.13.x        | 17         | 2.x         |
| 2.x.x            | 17.3 LTS          | 4.0.x         | 4.0.x              | 2.13.x        | 17         | 2.x         |

## Installation

Add db-zpark to your SBT project:

```scala
libraryDependencies += "io.github.fernanluyano" %% "db-zpark" % "x.y.z"
```

Where `x.y.z` is the latest version available on [Maven Central](https://central.sonatype.com/artifact/io.github.fernanluyano/db-zpark_2.12).

## Usage

### Why use this instead of using Jar tasks within a workflow?

Consider these common scenarios:

- **Dynamic table processing**: You need to ingest data for a dynamically growing number of tables
- **Configuration-driven workflows**: You have a fixed but large number of tables defined in a config file
- **Simplified orchestration**: Managing numerous tasks in Databricks Workflows becomes unwieldy
- **Code reusability**: You want to apply consistent patterns across multiple data processing jobs

With db-zpark, you can solve these challenges using a code-first approach that leverages dependency injection, reusable components, and structured logging.

#### Decision Guide

- **Have sequential tasks with dependencies?** → Use [Sequential Subtasks](#2-sequential-subtasks)
- **Have independent tasks that can run in parallel?** → Use [Parallel Subtasks](#3-parallel-subtasks)
- **Have a simpler workflow with few steps?** → Use [Simple Workflow Task](#1-simple-workflow-task)

### Common Use Cases and Examples

The core of db-zpark is the `WorkflowTask` trait, which provides a structured way to create Spark jobs with built-in logging and error handling:

#### 1. Simple Workflow Task

A basic workflow that reads, processes, and writes data.

This example optionally includes logging with Kafka integration for remote monitoring. Use this approach for straightforward data processing tasks or as a foundation for more complex workflows.

[See example](examples/BasicWorkflowTaskExample.scala)

#### 2. Sequential Subtasks

Process multiple tables in sequence with clear, reusable subtasks.

This approach works best when tasks have dependencies or must be processed in a specific order. Each subtask follows the same ETL pattern, though the `transform` function can be customized for different business logic.

[See example](examples/BatchDeltaTablesSequentialExample.scala)

#### 3. Parallel Subtasks

Process multiple tables concurrently for better performance.

This approach is ideal for independent tasks where parallel execution can significantly reduce overall processing time. It uses the same subtask structure as the sequential example but leverages concurrent execution.

[See example](examples/BatchDeltaTablesParallelExample.scala)

### Configuring a JAR Task in Databricks Workflow

To run your WorkflowTask as a JAR task in a Databricks workflow:

1. **Important: Your WorkflowTask implementation must be an object** (just like a traditional main function object), not a class. For example:
   ```scala
   // Correct implementation - as an object
   object MyDataProcessor extends WorkflowTask {
     // implementation
   }
   
   // Incorrect - this won't work as a JAR task
   class MyDataProcessor extends WorkflowTask {
     // implementation
   }
   ```

2. **Build your JAR**: Using SBT for example.

3. **Upload your JAR to Databricks**: S3, Volumes, etc.

4. **Create a JAR task in your workflow**:
    - Create a new JAR task in your Databricks workflow
    - Select the uploaded JAR
    - Set the Main class name to your object that extends WorkflowTask
      (e.g., `com.mycompany.myapp.MyDataProcessor`)
    - Like before, you can create several tasks pointing at different entry points (main classes) extending WorkflowTask, if desired.
5. **Configure cluster settings** as needed for your workload

This approach allows you to take advantage of Databricks workflow orchestration while leveraging all the benefits of db-zpark's structured task architecture.

### Configuring Logging (optional)

db-zpark provides a flexible logging system with console and Kafka options. This example creates a default logger
that sends the output in JSON format to the console and also to a Kafka topic in MSK (assuming your config is correct).
To enable this, just override the `bootstrap` val. If not specified, it will use the default console logger only.


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