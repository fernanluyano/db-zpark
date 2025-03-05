package dev.fb.dbzpark

import org.apache.spark.sql.SparkSession

/**
 * Dependencies for the [[WorkflowTask]]. Since it's a Spark application, at least the [[SparkSession]] and application
 * name should be provided.
 */
trait TaskEnvironment {

  /**
   * Clients will provide a custom-built [[SparkSession]].
   */
  def sparkSession: SparkSession

  /**
   * The spark application name.
   */
  def appName: String
}
