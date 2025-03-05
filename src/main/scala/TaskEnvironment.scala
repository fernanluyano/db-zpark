package dev.fb.dbzpark

import org.apache.spark.sql.SparkSession

/**
 * Clients will provide their own dependencies.
 */
trait TaskEnvironment {

  def sparkSession: SparkSession

  /**
   * As in "dev", "test", "prod" etc.
   */
  def environmentName: String

  /**
   * The spark application name.
   */
  def appName: String
}
