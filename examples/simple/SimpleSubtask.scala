import subtask.{SimpleContext, WorkflowSubtask}
import unitycatalog.Tables.UcTable

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.current_timestamp

/**
 * Example implementation of a WorkflowSubtask that reads from a source table, transforms the data, and writes to a
 * target table.
 *
 * Note: Source/target tables are just one possible use case. Subtasks can implement any arbitrary logic - API calls,
 * computations, orchestration, etc. - by overriding the trait methods.
 *
 * @param ignoreAndLogFailures
 *   If true, failures are logged but don't fail the workflow
 * @param name
 *   Unique identifier for this subtask
 * @param sourceTable
 *   Unity Catalog table to read from (example - not required for all subtasks)
 * @param targetTable
 *   Unity Catalog table to write to (example - not required for all subtasks)
 */
class SimpleSubtask(
  override protected val ignoreAndLogFailures: Boolean,
  val name: String,
  private val sourceTable: UcTable,
  private val targetTable: UcTable
) extends WorkflowSubtask {

  override def getContext: SimpleContext = SimpleContext(name)

  /**
   * Optional pre-processing step. Override only if needed for setup tasks.
   */
  override protected def preProcess(env: TaskEnvironment): Unit =
    println("optional")

  /**
   * Optional post-processing step. Override only if needed for cleanup tasks.
   */
  override protected def postProcess(env: TaskEnvironment): Unit =
    println("optional")

  /**
   * Reads data from the source Delta table.
   */
  override protected def readSource(env: TaskEnvironment): Dataset[_] =
    env.sparkSession.read
      .format("delta")
      .table(sourceTable.getFullyQualifiedName)

  /**
   * Transforms the input data by adding an ingestion timestamp.
   */
  override protected def transformer(env: TaskEnvironment, inDs: Dataset[_]): Dataset[_] =
    inDs.withColumn("_ingestion_time", current_timestamp())

  /**
   * Writes the transformed data to the target table.
   */
  override protected def sink(env: TaskEnvironment, outDs: Dataset[_]): Unit =
    outDs.writeTo(targetTable.getFullyQualifiedName)
      .option("checkpointLocation", checkpoint_path)
      .trigger(availableNow=True)
      .toTable("dev_catalog.dev_database.dev_table"))
}
