package dev.fb.dbzpark
package example.concurent

import logging.DefaultLogging
import subtask.ConcurrentRunner.GROUP_DEPENDENCIES
import subtask.ExecutionModel
import unitycatalog.Catalogs.UcCatalog
import unitycatalog.Schemas.UcSchema
import unitycatalog.Tables
import unitycatalog.Tables.UcTable

import org.apache.spark.sql.SparkSession

/**
 * A job that runs to a bunch of delta tables by ingesting data from S3 json files (streaming)
 */
object MyApp2 extends WorkflowTask with DefaultLogging {

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
   * assuming we have some dependencies
   */
  override protected def getExecutionModel(env: TaskEnvironment): ExecutionModel = {
    val baseLocation = "s3://autoloader-source/json-data"

    // This is a silly way of partitioning the tasks in groups. In a real world
    // example the developer should have an algorith to determine this.
    // In such case, tasks within the group will run concurrently while groups
    // will run in alphabetical order one after the other
    val subtasks = (1 to 10).map { i =>
      val table = s"table_$i"
      val group = {
        if(i < 3) "1"
        else if (i < 6) "2"
        else "3"
      }
      new S3StreamingTask2(
        ignoreAndLogFailures = false, // fail on the first failure found
        name = s"MyS3Task_$table",
        groupName = group,
        sourceS3Location = s"$baseLocation/$table",
        targetTable = s"catalog1.schema1.$table"
      )
    }
    // Provide an Executor or let the framework create one for you
    // export NUM_THREADS for a custom number of threads (defaults to 4)
    ExecutionModel.concurrent(subtasks, GROUP_DEPENDENCIES, maxRunningTasks = 2)
  }

  /** Optional target table for log persistence */
  override val logsTable: Option[Tables.UcTable] = {
    val devCatalog = new UcCatalog {
      override def getSimpleName: String = "dev"
    }
    val engSchema = new UcSchema {
      override val catalog: UcCatalog = devCatalog
      override def getSimpleName: String = "engineering"
    }

    Some(new UcTable {
      override val schema: UcSchema = engSchema
      override def getSimpleName: String = "app_logs"
    })
  }
}
