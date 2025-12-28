package dev.fb.dbzpark
package spark

import spark.BuildProperty.SparkBuildProperty

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Fluent builder for creating SparkSessions with type-safe build property configuration.
 *
 * Example:
 * {{{
 *   SparkSessionBuilder()
 *     .set(Master(Some("local[*]")))
 *     .set(AppName(Some("MyApp")))
 *     .set(Serializer(Some("org.apache.spark.serializer.KryoSerializer")))
 *     .build
 * }}}
 */
class SparkSessionBuilder {
  private val conf = new SparkConf()

  /**
   * Sets a single build property on the SparkConf.
   * @param property the build property to configure
   * @return this builder instance for method chaining
   */
  def set(property: SparkBuildProperty): SparkSessionBuilder = {
    conf.set(property.getName, property.getValue)
    this
  }

  /**
   * Sets multiple build properties in a single call.
   * @param properties sequence of build properties to configure
   * @return this builder instance for method chaining
   */
  def setAll(properties: Seq[SparkBuildProperty]): SparkSessionBuilder = {
    properties.foreach(p => conf.set(p.getName, p.getValue))
    this
  }

  /**
   * Creates the SparkSession using the accumulated configuration.
   *
   * @return a SparkSession configured with the accumulated build properties
   */
  def build: SparkSession =
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
}

object SparkSessionBuilder {

  /**
   * Factory method for creating a new builder instance.
   * @return a new SparkSessionBuilder with empty configuration
   */
  def apply(): SparkSessionBuilder = new SparkSessionBuilder
}
