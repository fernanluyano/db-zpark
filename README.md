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
- Sequential and concurrent task execution with configurable parallelism

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
libraryDependencies += "io.github.fernanluyano" %% "db-zpark" % "2.1.1"
```

[Maven Central](https://central.sonatype.com/artifact/io.github.fernanluyano/db-zpark_2.13).

## Spark Session Configuration

Use more convenient and safer alterntives for configuring Spark sessions with validation at construction time.

### Building a Spark Session

```scala
import spark.BuildProperty._
import spark.SparkSessionBuilder

val spark = SparkSessionBuilder()
  .set(Master(Some("local[*]")))
  .set(AppName(Some("MyApp")))
  .set(Serializer(Some("org.apache.spark.serializer.KryoSerializer")))
  .build
```

### Modifying Runtime Configuration

```scala
import spark.RuntimeProperty._
import spark.SparkPropertySetter

SparkPropertySetter(spark)
  .set(MaxFilesPerTrigger(Some("500")))
  .set(MaxBytesPerTrigger(Some("2g")))
```

## Usage

### Why use this instead of using Jar tasks within a workflow?

Consider these common scenarios:

- **Dynamic table processing**: You need to ingest data for a dynamically growing number of tables
- **Configuration-driven workflows**: You have a fixed but large number of tables defined in a config file
- **Simplified orchestration**: Managing numerous tasks in Databricks Workflows becomes unwieldy
- **Code reusability**: You want to apply consistent patterns across multiple data processing jobs

With db-zpark, you can solve these challenges using a code-first approach that leverages dependency injection, reusable components, and structured logging.

#### Decision Guide

Choose your execution pattern based on your workflow requirements:

- **Have a simple workflow with few steps?** → Use [Simple Workflow Task](#1-simple-workflow-task)
- **Need to process tasks one after another?** → Use [Sequential Subtasks](#2-sequential-subtasks)
- **Have independent tasks that can run in parallel?** → Use [Concurrent Subtasks](#3-concurrent-subtasks)
    - All tasks can run at once? → Use `NO_DEPENDENCIES` strategy
    - Tasks grouped with dependencies between groups? → Use `GROUP_DEPENDENCIES` strategy

### Common Use Cases and Examples

The core of db-zpark is the `WorkflowTask` trait, which provides a structured way to create Spark jobs with built-in logging and error handling.

#### 1. Simple Workflow Task

A basic workflow that reads, processes, and writes data.

This example optionally includes logging with Kafka integration for remote monitoring. Use this approach for straightforward data processing tasks or as a foundation for more complex workflows.

**Example**: [examples/simple/SimpleApp.scala](examples/simple/SimpleApp.scala)

#### 2. Sequential Subtasks

Process multiple tasks in sequence with clear, reusable subtasks.

This approach works best when tasks have dependencies or must be processed in a specific order. Each subtask follows a structured ETL pattern (pre-process → read → transform → sink → post-process), though each stage can be customized for different business logic.

**Example**: [examples/sequential/MyApp.scala](examples/sequential/MyApp.scala)

#### 3. Concurrent Subtasks

Process multiple tasks concurrently for better performance.

This approach offers two execution strategies:

**3a. NO_DEPENDENCIES Strategy**

All tasks run in parallel with configurable parallelism limits. Ideal when all tasks are independent and can execute simultaneously.

**Example**: [examples/concurrent/MyApp1.scala](examples/concurrent/MyApp1.scala)

**3b. GROUP_DEPENDENCIES Strategy**

Tasks are organized into groups. Tasks within a group run concurrently, but groups execute sequentially in alphabetical order. Perfect for workflows where you have sets of independent tasks with dependencies between those sets.

**Example**: [examples/concurrent/MyApp2.scala](examples/concurrent/MyApp2.scala)

##### Parallelism Control

The concurrent runner provides fine-grained control over parallelism:

```scala
// maxRunningSubtasks controls fiber-level concurrency (how many tasks run at once)
val runner = ConcurrentRunner(subtasks, strategy, maxRunningSubtasks = 8)

// Executor controls thread pool size (for blocking operations)
val executor = Executor.fromJavaExecutor(Executors.newFixedThreadPool(4))
val model = new ExecutionModel(runner, Some(executor))
```

**Key distinction**:
- `maxRunningSubtasks`: Controls how many ZIO fibers run concurrently (lightweight)
- `executor thread pool`: Controls how many OS threads are available (heavyweight)

Many fibers can run on few threads, making it possible to have high concurrency (100+ tasks) on a small thread pool (4-8 threads).

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
    - You can create several tasks pointing at different entry points (main classes) extending WorkflowTask, if desired.
5. **Configure cluster settings** as needed for your workload

This approach allows you to take advantage of Databricks workflow orchestration while leveraging all the benefits of db-zpark's structured task architecture.

### Configuring Logging (optional)

db-zpark provides a flexible logging system with console and Kafka options. To enable custom logging,
create a trait that extends `DefaultLogging` and mix it into your WorkflowTask. The default implementation provides JSON console logging out of the box.
```scala
import logging.DefaultLogging

import org.apache.spark.sql.SparkSession
import zio.{ZIO, ZIOAppArgs, ZLayer}

object MySparkJobWithLogging extends WorkflowTask with DefaultLogging {
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
