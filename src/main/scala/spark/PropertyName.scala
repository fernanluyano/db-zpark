package dev.fb.dbzpark
package spark

/**
 * Constants for common property names.
 */
object PropertyName {
  val APP_NAME                  = "spark.app.name"
  val MASTER                    = "spark.master"
  val MAX_BYTES_PER_TRIGGER     = "maxBytesPerTrigger"
  val MAX_FILES_PER_TRIGGER     = "maxFilesPerTrigger"
  val SCHEDULER_ALLOCATION_FILE = "spark.scheduler.allocation.file"
  val SCHEDULER_MODE            = "spark.scheduler.mode"
  val SERIALIZER                = "spark.serializer"
}
