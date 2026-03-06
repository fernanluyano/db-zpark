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
- DAG-based task scheduling with topological ordering, priority dispatch, and configurable parallelism

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

- **Single task or very simple pipeline?** → Use [Simple Workflow Task](#1-simple-workflow-task)
- **Independent tasks that can run in parallel?** → Use [Parallel Subtasks](#2-parallel-subtasks)
- **Tasks with ordering dependencies?** → Use [DAG Subtasks](#3-dag-subtasks)

### Common Use Cases and Examples

The core of db-zpark is the `WorkflowTask` trait. Extend it, implement `buildTaskEnvironment`
to supply a `SubtasksGraph` and execution settings, and db-zpark handles the rest.

#### 1. Simple Workflow Task

A single subtask registered in a one-node graph. Use this as the starting point for straightforward pipelines.

**Example**: [examples/simple/MyApp.scala](examples/simple/MyApp.scala)

#### 2. Parallel Subtasks

Multiple independent subtasks registered in a graph with no dependencies between them.
All nodes are ready in the first batch and run concurrently up to `maxConcurrentSubtasks`.

**Example**: [examples/parallel/MyApp.scala](examples/parallel/MyApp.scala)

#### 3. DAG Subtasks

Subtasks with explicit parent–child dependencies declared via `ParentChildDependency`.
The scheduler uses topological ordering (Kahn's algorithm) to dispatch nodes in dependency order.
Nodes in the same batch run concurrently; nodes in later batches wait for their parents.

Failure behaviour is controlled by `failFast` on `TaskEnvironment`:
- `failFast = false` (default): a failed subtask logs the error, skips its descendants, and lets unrelated subtasks continue.
- `failFast = true`: the first failure halts the entire run immediately.

**Example**: [examples/dag/MyApp.scala](examples/dag/MyApp.scala)

##### Post-Processing

Each `WorkflowSubtask` exposes an optional `postProcess` hook that runs after the pipeline (`readSource` → `transformer` → `sink`). It can be used for anything: cleanup, notifications, metadata updates, etc.

By default, `postProcess` only runs on success. Override `ensurePostProcess` to `true` to guarantee it runs regardless of pipeline outcome — the original error is still re-raised afterwards:

```scala
class MySubtask extends WorkflowSubtask {
  override val taskId: String            = "my-task"
  override val ensurePostProcess: Boolean = true  // postProcess runs even on failure

  override protected def postProcess(env: TaskEnvironment): Task[Unit] =
    ZIO.attempt(cleanUpTempFiles())

  // ... readSource, transformer, sink
}
```

> **Note**: `run` is `final`. Customise behaviour exclusively through the provided hooks
> (`preProcess`, `readSource`, `transformer`, `sink`, `postProcess`).

##### Parallelism Control

Two independent settings in `TaskEnvironment` control concurrency:

```scala
new TaskEnvironment {
  // How many subtasks may run concurrently within one batch
  override def maxConcurrentSubtasks: Int = 8

  // Thread pool backing the ZIO executor (for blocking Spark operations)
  override def subtasksExecutor: Executor =
    Executor.fromJavaExecutor(Executors.newFixedThreadPool(4))
  ...
}
```

**Key distinction**:
- `maxConcurrentSubtasks`: controls how many ZIO fibers run concurrently (lightweight).
- `subtasksExecutor` thread pool: controls how many OS threads are available (heavyweight, for blocking Spark operations).

Many fibers can run on few threads, making it possible to have high concurrency (100+ tasks) on a small thread pool.

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

db-zpark provides a flexible logging system with console and JSON file options. Mix `DefaultLogging`
into your `WorkflowTask` to override the ZIO `bootstrap` layer with both console and file-based JSON
logging. Set `logsTable` to persist logs to a Delta table after the run completes.

```scala
import logging.DefaultLogging
import unitycatalog.Tables.UcTable

import org.apache.spark.sql.SparkSession
import zio.Executor

import java.util.concurrent.Executors

object MySparkJobWithLogging extends WorkflowTask with DefaultLogging {

  // Set to Some(UcTable(...)) to persist logs to a Delta table after the run
  override val logsTable: Option[UcTable] = None

  override protected def buildTaskEnvironment: TaskEnvironment = {
    val spark = SparkSession.builder()
      .appName("my-spark-job")
      .getOrCreate()

    val graph = subtask.SubtasksGraph.Builder()
      .addSubtask(/* your subtasks */)
      .build

    new TaskEnvironment {
      override def sparkSession: SparkSession = spark
      override def appName: String            = "my-spark-job"
      override def subtasksGraph              = graph
      override def subtasksExecutor: Executor = Executor.fromJavaExecutor(Executors.newFixedThreadPool(4))
      override def maxConcurrentSubtasks: Int = 4
    }
  }
}
```

The log file is written to `logFilePath` (default: `file:///tmp/db_zpark_logs.log`).
Override `logFilePath` in your object to change the location.

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
