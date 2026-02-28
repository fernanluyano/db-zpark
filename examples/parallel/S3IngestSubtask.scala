package dev.fb.dbzpark
package example.parallel

import subtask.WorkflowSubtask

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.streaming.Trigger
import zio.{Task, ZIO}

/**
 * Reads a JSON stream from S3 using Autoloader, adds an ingestion timestamp,
 * and writes the result to a Delta table using trigger-available-now semantics.
 *
 * Parameterised so that a single class definition can back many independent
 * subtasks in the same graph (see [[MyApp]]).
 *
 * @param taskId          Unique identifier used for logging and graph registration.
 * @param localPriority   Dispatch priority relative to other ready subtasks (higher = first).
 * @param sourceS3Path    S3 path passed to the Autoloader cloudFiles source.
 * @param targetTable     Fully-qualified Delta table name ({catalog}.{schema}.{table}).
 */
class S3IngestSubtask(
  override val taskId: String,
  override val localPriority: Int = 1,
  private val sourceS3Path: String,
  private val targetTable: String
) extends WorkflowSubtask {

  override protected def readSource(env: TaskEnvironment): Task[Dataset[_]] =
    ZIO.attempt {
      env.sparkSession.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(sourceS3Path)
    }

  override protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
    ZIO.attempt(inDs.withColumn("_ingestion_time", current_timestamp()))

  override protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] =
    ZIO.attempt {
      outDs.writeStream
        .format("delta")
        .option("checkpointLocation", s"s3://checkpoints/$targetTable")
        .trigger(Trigger.AvailableNow())
        .toTable(targetTable)
        .awaitTermination()
    }
}
