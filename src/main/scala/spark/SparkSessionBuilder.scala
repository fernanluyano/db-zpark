package dev.fb.dbzpark
package spark

import spark.BuildProperty.SparkBuildProperty

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkSessionBuilder {
  private val conf = new SparkConf()

  def set(property: SparkBuildProperty): SparkSessionBuilder = {
    conf.set(property.getName, property.getValue)
    this
  }

  def setAll(properties: Seq[SparkBuildProperty]): SparkSessionBuilder = {
    properties.foreach(p => conf.set(p.getName, p.getValue))
    this
  }

  def build: SparkSession = {
    SparkSession.builder()
      .config(conf)
      .getOrCreate()
  }
}

object SparkSessionBuilder {
  def apply(): SparkSessionBuilder = new SparkSessionBuilder
}

