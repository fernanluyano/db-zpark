package dev.fb.dbzpark
package spark

/**
 * Runtime Spark properties that can be modified after SparkSession initialization.
 *
 * Unlike BuildProperty, these configurations can be changed dynamically during execution
 * via spark.conf.set(). This is crucial for streaming workloads where throttling and
 * resource management often need adjustment based on observed behavior without restarting
 * the entire session.
 */
object RuntimeProperty {

  trait SparkRuntimeProperty extends SparkProperty

  /**
   * Maximum number of files to process per streaming micro-batch.
   *
   * Defaults to 1000 to prevent overwhelming the driver with file listing operations in
   * scenarios with many small files. Lower this if you're seeing driver memory pressure
   * or long micro-batch planning times. Raise it if you have fewer, larger files and want
   * faster catch-up during backfill.
   */
  final case class MaxFilesPerTrigger(override val value: Option[String]) extends SparkRuntimeProperty {
    override protected val name: String                 = "maxFilesPerTrigger"
    override protected val defaultValue: Option[String] = Some("1000")
  }

  /**
   * Maximum data volume to process per streaming micro-batch.
   *
   * Defaults to 1g to provide predictable, manageable batch sizes. This is often the primary
   * throttle for streaming workloads - it prevents a single micro-batch from consuming too
   * much memory or taking too long to process. Use this in conjunction with MaxFilesPerTrigger;
   * whichever limit is hit first determines the batch boundary.
   */
  final case class MaxBytesPerTrigger(override val value: Option[String]) extends SparkRuntimeProperty {
    override protected val name: String                 = "maxBytesPerTrigger"
    override protected val defaultValue: Option[String] = Some("1g")
  }

  /**
   * Escape hatch for arbitrary runtime properties not explicitly modeled.
   *
   * Allows setting any runtime configuration without requiring code changes for every
   * streaming option or Databricks-specific parameter. Use sparingly - prefer typed
   * properties for better compile-time safety and discoverability. Common use cases
   * include Auto Loader (cloudFiles.*) options and format-specific configurations.
   */
  final case class AnyRuntimeProperty(
    override val name: String,
    override val value: Option[String],
    override val defaultValue: Option[String]
  ) extends SparkRuntimeProperty
}
