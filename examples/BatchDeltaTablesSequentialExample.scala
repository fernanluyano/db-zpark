package examples

import subtask.{Factory, SubtaskContext, WorkflowSubtask}

import dev.fb.dbzpark.{TaskEnvironment, WorkflowTask}
import org.apache.spark.sql.{Dataset, SparkSession}
import zio._

/**
 * Example demonstrating simple batch processing subtasks for multiple delta tables.
 * Tables are discovered dynamically.
 */
object BatchDeltaTablesSequentialExample extends WorkflowTask {

  /**
   * Basic task environment with additional configuration.
   */
  class DeltaTaskEnvironment(
    val sparkSession: SparkSession,
    val appName: String,
    val sourceBasePath: String,
    val targetBasePath: String
  ) extends TaskEnvironment

  /**
   * Build the task environment with paths from configuration.
   */
  override protected def buildTaskEnvironment: TaskEnvironment = {
    val spark = SparkSession
      .builder()
      .appName("Dynamic Delta Tables Processor")
      .master("local[*]")
      .getOrCreate()
    val sourceBasePath = "s3://data-lake/raw"
    val targetBasePath = "s3://data-lake/silver"

    new DeltaTaskEnvironment(
      spark,
      "Dynamic Delta Tables Processor",
      sourceBasePath,
      targetBasePath
    )
  }

  /**
   * Main task logic to discover tables and set up subtasks.
   */
  override protected def startTask: ZIO[TaskEnvironment, Throwable, Unit] =
    for {
      // Cast to our specific environment type for access to config
      env      <- ZIO.serviceWith[TaskEnvironment](_.asInstanceOf[DeltaTaskEnvironment])
      _        <- ZIO.logInfo(s"Starting dynamic table discovery from ${env.sourceBasePath}")
      subtasks <- ZIO.attempt(getSubtasks(env))
      _        <- ZIO.logInfo(s"Discovered ${subtasks.length} tables")
      _        <- runSubtasks(subtasks)
      _        <- ZIO.logInfo("All discovered delta tables processed successfully")
    } yield ()

  // Assume table names are gotten dynamically from a config, API or registry
  private def getSubtasks(env: DeltaTaskEnvironment): Seq[DeltaTableSubtask] =
    (1 to 10).map(i => s"table_$i").map { t =>
      new DeltaTableSubtask(
        tableName = t,
        sourcePath = s"${env.sourceBasePath}/$t",
        targetPath = s"${env.targetBasePath}/$t"
      )
    }

  private def runSubtasks(subtasks: Seq[DeltaTableSubtask]) =
    // Handle the case where no tables were found
    if (subtasks.isEmpty) {
      ZIO.logWarning("No tables found to process")
    } else {
      // Create and run the sequential runner
      val runner = Factory.SequentialRunner(subtasks)
      runner.run
    }

  /**
   * Simple subtask to process one delta table.
   */
  class DeltaTableSubtask(
    tableName: String,
    sourcePath: String,
    targetPath: String
  ) extends WorkflowSubtask {
    // ignore failures for a given task and continue
    override protected val ignoreAndLogFailures: Boolean = tableName == "table_2"

    override val context: SubtaskContext = SubtaskContext(s"process-$tableName", 1)

    override def readSource(env: TaskEnvironment): Dataset[_] =
      // Read from source path
      env.sparkSession.read
        .format("parquet") // Adjust format as needed
        .load(sourcePath)

    override def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] = {
      import org.apache.spark.sql.functions._
      // Apply minimal transformations and add metadata
      inDs
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("table_name", lit(tableName))
        .withColumn("source_path", lit(sourcePath))
    }

    override def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit =
      // Write to Delta table
      outDs.write
        .format("delta")
        .mode("overwrite") // or append, merge as needed
        .option("overwriteSchema", "true")
        .save(targetPath)
  }
}
