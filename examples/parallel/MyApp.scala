package dev.fb.dbzpark
package example.parallel

import subtask.SubtasksGraph

import org.apache.spark.sql.SparkSession
import zio.Executor

import java.util.concurrent.Executors

/**
 * Parallel example: three independent S3 ingest subtasks with no dependencies
 * between them. The graph schedules all three in the first batch and runs them
 * concurrently up to [[maxConcurrentSubtasks]].
 * To model sequential execution instead, add dependencies between the nodes
 * using [[subtask.SubtasksGraph.Builder.addDependency]]. See the `dag` example
 * for a worked dependency graph.
 */
object MyApp extends WorkflowTask {

  private val baseS3Path = "s3://autoloader-source/json-data"
  private val tables     = Seq("table_1", "table_2", "table_3")

  override protected def buildTaskEnvironment: TaskEnvironment = {

    val builder = tables.foldLeft(SubtasksGraph.Builder()) { (b, table) =>
      b.addSubtask(new S3IngestSubtask(
        taskId       = s"ingest-$table",
        sourceS3Path = s"$baseS3Path/$table",
        targetTable  = s"catalog1.schema1.$table"
      ))
    }

    val graph = builder.build

    new TaskEnvironment {
      override def sparkSession: SparkSession = SparkSession.builder().appName(appName).getOrCreate()
      override def appName: String            = "parallel-s3-ingest"
      override def subtasksGraph              = graph
      override def subtasksExecutor: Executor = Executor.fromJavaExecutor(Executors.newFixedThreadPool(3))
      override def maxConcurrentSubtasks: Int = 3
    }
  }
}
