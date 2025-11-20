package dev.fb.dbzpark
package examples.concurrent

import subtask.ConcurrentRunner.NO_DEPENDENCIES
import subtask.ExecutionModel

import org.apache.spark.sql.SparkSession

/**
 * A job that runs to a bunch of delta tables by ingesting data from S3 json files (streaming)
 */
object MyApp1 extends WorkflowTask {

  override protected def buildTaskEnvironment: TaskEnvironment = new TaskEnvironment {
    override def sparkSession: SparkSession = SparkSession
      .builder()
      .appName(appName)
      .master("local[2]") // Run locally with 2 cores
      .getOrCreate()

    override def appName: String = "MyApp"
  }

  /**
   * Declare several tasks that have the same template, and run them concurrently
   * assuming no dependencies
   */
  override protected def getExecutionModel(env: TaskEnvironment): ExecutionModel = {
    val baseLocation = "s3://autoloader-source/json-data"

    val subtasks = Seq("table_1", "table_2", "table_3").map { t =>
      new S3StreamingTask(
        ignoreAndLogFailures = true, // DON't fail on the first failure found
        name = s"MyS3Task_$t",
        sourceS3Location = s"$baseLocation/$t",
        targetTable = s"catalog1.schema1.$t"
      )
    }
    // Provide an Executor or let the framework create one for you
    // export NUM_THREADS for a custom number of threads (defaults to 4)
    ExecutionModel.concurrent(subtasks, NO_DEPENDENCIES, maxRunningTasks = 6)
  }
}
