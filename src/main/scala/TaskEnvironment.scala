package dev.fb.dbzpark

import dev.fb.dbzpark.subtask.SubtasksGraph
import org.apache.spark.sql.SparkSession
import zio.Executor

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

  def subtasksGraph: SubtasksGraph

  def subtasksExecutor: Executor

  def maxConcurrentSubtasks: Int
}
