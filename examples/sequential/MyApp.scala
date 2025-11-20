package dev.fb.dbzpark
package examples.sequential

import subtask.ExecutionModel

import org.apache.spark.sql.SparkSession

/**
 * A job that runs to a bunch of delta tables by ingesting data from S3 json files (streaming)
 */
object MyApp extends WorkflowTask {

  override protected def buildTaskEnvironment: TaskEnvironment = new TaskEnvironment {
    override def sparkSession: SparkSession = SparkSession
      .builder()
      .appName(appName)
      .master("local[2]") // Run locally with 2 cores
      .getOrCreate()

    override def appName: String = "MyApp"
  }

  /**
   * Declare several tasks that have the same template, and run them sequentially
   */
  override protected def getExecutionModel: ExecutionModel = {
    val baseLocation = "s3://autoloader-source/json-data"

    val subtasks = Seq("table_1", "table_2", "table_3").map { t =>
      new S3StreamingTask(
        ignoreAndLogFailures = false, // fail on the first failure found
        name = s"MyS3Task_$t",
        sourceS3Location = s"$baseLocation/$t",
        targetTable = s"catalog1.schema1.$t"
      )
    }

    ExecutionModel.sequential(subtasks)
  }
}
