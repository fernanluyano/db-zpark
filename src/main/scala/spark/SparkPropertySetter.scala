package dev.fb.dbzpark
package spark

import spark.RuntimeProperty.SparkRuntimeProperty

import org.apache.spark.sql.SparkSession

/**
 * Fluent API for setting runtime properties on an active SparkSession.
 *
 * Example:
 * {{{
 *   SparkPropertySetter(spark)
 *     .set(MaxFilesPerTrigger(Some("500")))
 *     .set(MaxBytesPerTrigger(Some("2g")))
 * }}}
 */
class SparkPropertySetter(private val spark: SparkSession) {

  /**
   * Sets a runtime property on the active SparkSession.
   * @param property the runtime property to set
   * @return this setter instance for method chaining
   */
  def set(property: SparkRuntimeProperty): SparkPropertySetter = {
    spark.conf.set(property.getName, property.getValue)
    this
  }

  /**
   * Sets several properties on the active SparkSession.
   * @param properties the runtime properties to set
   * @return this setter instance for method chaining
   */
  def setAll(properties: Seq[SparkRuntimeProperty]): SparkPropertySetter = {
    properties.foreach(p => spark.conf.set(p.getName, p.getValue))
    this
  }
}

object SparkPropertySetter {

  /**
   * Factory method for creating a property setter from an existing SparkSession.
   * @param spark the active SparkSession to configure
   * @return a new SparkPropertySetter instance
   */
  def apply(spark: SparkSession): SparkPropertySetter = new SparkPropertySetter(spark)
}
