package dev.fb.dbzpark
package example.dag

import subtask.{ParentChildDependency, SubtasksGraph}

import org.apache.spark.sql.SparkSession
import zio.Executor

import java.util.concurrent.Executors

/**
 * DAG example: four subtasks with explicit parent-child dependencies forming
 * a diamond pattern. The two bronze ingests run in parallel; the silver join
 * waits for both; the gold aggregate waits for the join.
 *
 * Graph topology:
 *
 * {{{
 *   bronze-ingest-accounts (LP:10)   bronze-ingest-opportunities (LP:10)
 *                    \                        /
 *                     +-->  silver-join (LP:5)
 *                                  |
 *                        gold-aggregate (LP:1)
 * }}}
 *
 * Failure behaviour: [[failFast]] is false (the default), so a failed bronze
 * ingest logs the error and skips its downstream dependents while letting any
 * unrelated subtasks continue. Set [[failFast]] to true to abort the entire
 * run on the first failure.
 */
object MyApp extends WorkflowTask {

  override protected def buildTaskEnvironment: TaskEnvironment = {

    val ingestAccounts      = new BronzeIngestSubtask(
      taskId        = "bronze-ingest-accounts",
      localPriority = 10,
      sourceS3Path  = "s3://raw-data/accounts",
      bronzeTable   = "dev.bronze.accounts"
    )
    val ingestOpportunities = new BronzeIngestSubtask(
      taskId        = "bronze-ingest-opportunities",
      localPriority = 10,
      sourceS3Path  = "s3://raw-data/opportunities",
      bronzeTable   = "dev.bronze.opportunities"
    )
    val silverJoin          = new SilverJoinSubtask
    val goldAggregate       = new GoldAggregateSubtask

    val graph = SubtasksGraph.Builder()
      .addSubtask(ingestAccounts)
      .addSubtask(ingestOpportunities)
      .addSubtask(silverJoin)
      .addSubtask(goldAggregate)
      .addDependencies(Seq(
        ParentChildDependency("bronze-ingest-accounts",      "silver-join"),
        ParentChildDependency("bronze-ingest-opportunities", "silver-join"),
        ParentChildDependency("silver-join",                 "gold-aggregate")
      ))
      .build

    new TaskEnvironment {
      override def sparkSession: SparkSession = SparkSession.builder().appName(appName).getOrCreate()
      override def appName: String            = "accounts-pipeline"
      override def subtasksGraph              = graph
      override def subtasksExecutor: Executor = Executor.fromJavaExecutor(Executors.newFixedThreadPool(4))
      override def maxConcurrentSubtasks: Int = 4
    }
  }
}
