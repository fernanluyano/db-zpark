package dev.fb.dbzpark
package example.dag

import subtask.WorkflowSubtask

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{count, sum}
import zio.{Task, ZIO}

/**
 * Aggregates the silver join result into a gold summary table.
 *
 * Must run after [[SilverJoinSubtask]]. The dependency is declared in [[MyApp]].
 */
class GoldAggregateSubtask extends WorkflowSubtask {
  override val taskId: String     = "gold-aggregate"
  override val localPriority: Int = 1

  override protected def readSource(env: TaskEnvironment): Task[Dataset[_]] =
    ZIO.attempt {
      env.sparkSession.read.format("delta").table("dev.silver.account_opportunities")
    }

  override protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
    ZIO.attempt {
      inDs.groupBy("account_id", "account_name")
        .agg(
          count("opportunity_id").as("opportunity_count"),
          sum("amount").as("total_amount")
        )
    }

  override protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] =
    ZIO.attempt {
      outDs.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("dev.gold.account_summary")
    }
}
