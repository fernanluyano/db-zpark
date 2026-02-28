package dev.fb.dbzpark
package example.simple

import subtask.{SubtasksGraph, WorkflowSubtask}

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.current_timestamp
import zio.{Executor, Task, ZIO}

import java.util.concurrent.Executors

/**
 * Minimal example: a single subtask that reads from a Delta bronze table,
 * adds an ingestion timestamp, and writes to a Delta silver table.
 *
 * This is the starting point for most pipelines. Once you need more than one
 * subtask, see the `parallel` or `dag` examples.
 */
object MyApp extends WorkflowTask {

  override protected def buildTaskEnvironment: TaskEnvironment = {

    val ingestAccounts = new WorkflowSubtask {
      override val taskId: String = "ingest-accounts"

      override protected def readSource(env: TaskEnvironment): Task[Dataset[_]] =
        ZIO.attempt {
          env.sparkSession.read
            .format("delta")
            .table("dev.salesforce_bronze.accounts")
        }

      override protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
        ZIO.attempt(inDs.withColumn("_ingestion_time", current_timestamp()))

      override protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] =
        ZIO.attempt {
          outDs.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable("dev.salesforce_silver.accounts")
        }
    }

    val graph = SubtasksGraph.Builder()
      .addSubtask(ingestAccounts)
      .build

    new TaskEnvironment {
      override def sparkSession: SparkSession = SparkSession.builder().appName(appName).getOrCreate()
      override def appName: String            = "ingest-accounts"
      override def subtasksGraph              = graph
      override def subtasksExecutor: Executor = Executor.fromJavaExecutor(Executors.newFixedThreadPool(1))
      override def maxConcurrentSubtasks: Int = 1
    }
  }
}
