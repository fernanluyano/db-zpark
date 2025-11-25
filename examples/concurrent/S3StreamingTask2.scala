package dev.fb.dbzpark
package example.concurent

import subtask.{GroupingContext, SubtaskContext, WorkflowSubtask}

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.streaming.Trigger

/**
 * A task to simulate reading json files from a S3 bucket with Autoloader
 */
class S3StreamingTask2(
                        private val name: String,
                        private val groupName: String,
                        override protected val ignoreAndLogFailures: Boolean,
                        private val sourceS3Location: String,
                        private val targetTable: String
                      ) extends WorkflowSubtask {

  /**
   * Optional pre-processing step. Override only if needed for setup tasks.
   */
  override protected def preProcess(env: TaskEnvironment): Unit = println("optional")

  /**
   * Optional post-processing step. Override only if needed for cleanup tasks.
   */
  override protected def postProcess(env: TaskEnvironment): Unit = println("optional")

  override def getContext: SubtaskContext = GroupingContext(name, groupName)

  /**
   * Stream json files using Autoloader
   */
  override protected def readSource(env: TaskEnvironment): Dataset[_] =
    env.sparkSession.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load(sourceS3Location)

  /**
   * Transforms the input data by adding an ingestion timestamp.
   */
  override protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] =
    inDs.withColumn("_ingestion_time", current_timestamp())

  /**
   * Write the stream to a delta table
   */
  override protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit =
    outDs.writeStream
      .format("delta")
      .option("checkpointLocation", s"s3://checkpoints/$targetTable")
      .trigger(Trigger.AvailableNow())
      .toTable(targetTable)
      .awaitTermination()
}
