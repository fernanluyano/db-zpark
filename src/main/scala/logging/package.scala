package dev.fb.dbzpark

import org.apache.spark.sql.types._

package object logging {

  /**
   * Creates and returns a Spark SQL schema that corresponds to the default configuration used.
   *
   * @return
   *   A StructType representing the Spark SQL schema for log entries
   */
  def getDefaultLogEntrySparkSchema: StructType =
    StructType(
      Seq(
        StructField("timestamp", TimestampType, nullable = true),
        StructField("level", StringType, nullable = true),
        StructField("thread", StringType, nullable = true),
        StructField("message", StringType, nullable = true),
        StructField("cause", StringType, nullable = true),
        StructField("logger_name", StringType, nullable = true),
        StructField("spans", MapType(StringType, StringType), nullable = true),
        StructField("custom_fields", MapType(StringType, StringType), nullable = true)
      )
    )
}
