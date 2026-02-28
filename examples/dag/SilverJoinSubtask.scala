package dev.fb.dbzpark
package example.dag

import subtask.WorkflowSubtask

import org.apache.spark.sql.Dataset
import zio.{Task, ZIO}

/**
 * Joins the bronze accounts and opportunities tables into a single silver dataset.
 *
 * This subtask must run after both [[BronzeIngestSubtask]] instances have
 * completed successfully. That ordering is enforced by the graph dependencies
 * declared in [[MyApp]]; this class has no awareness of it.
 */
class SilverJoinSubtask extends WorkflowSubtask {
  override val taskId: String    = "silver-join"
  override val localPriority: Int = 5

  override protected def readSource(env: TaskEnvironment): Task[Dataset[_]] =
    ZIO.attempt {
      env.sparkSession.read.format("delta").table("dev.bronze.accounts")
    }

  override protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Task[Dataset[_]] =
    ZIO.attempt {
      val opportunities = env.sparkSession.read.format("delta").table("dev.bronze.opportunities")
      inDs.join(opportunities, Seq("account_id"))
    }

  override protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Task[Unit] =
    ZIO.attempt {
      outDs.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("dev.silver.account_opportunities")
    }
}
