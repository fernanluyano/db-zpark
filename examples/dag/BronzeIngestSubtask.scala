package dev.fb.dbzpark
package example.dag

import subtask.WorkflowSubtask

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.current_timestamp
import zio.{Task, ZIO}

/**
 * Reads raw JSON from S3 using Autoloader and lands it in a Delta bronze table.
 *
 * @param taskId       Unique identifier used for logging and graph registration.
 * @param localPriority Dispatch priority relative to other ready subtasks (higher = first).
 * @param sourceS3Path  S3 path passed to the Autoloader cloudFiles source.
 * @param bronzeTable   Fully-qualified target Delta table ({catalog}.{schema}.{table}).
 */
class BronzeIngestSubtask(
  override val taskId: String,
  override val localPriority: Int = 1,
  private val sourceS3Path: String,
  private val bronzeTable: String
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
        .option("checkpointLocation", s"s3://checkpoints/$bronzeTable")
        .toTable(bronzeTable)
        .awaitTermination()
    }
}
