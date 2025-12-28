package dev.fb.dbzpark
package spark

import spark.RuntimeProperty.SparkRuntimeProperty

import org.apache.spark.sql.SparkSession

class SparkPropertySetter(private val spark: SparkSession) {
  def set(runtimeConfigProp: SparkRuntimeProperty): SparkPropertySetter = {
    spark.conf.set(runtimeConfigProp.getName, runtimeConfigProp.getValue)
    this
  }
}

object SparkPropertySetter {
  def apply(spark: SparkSession): SparkPropertySetter = new SparkPropertySetter(spark)
}
