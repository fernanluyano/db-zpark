package dev.fb.dbzpark
package spark

object BuildProperty {

  sealed trait SparkBuildProperty extends SparkProperty

  final case class Master(
    override val value: Option[String]
  ) extends SparkBuildProperty {
    override val name                         = "spark.master"
    override val defaultValue: Option[String] = None
  }

  final case class AppName(
    override val value: Option[String]
  ) extends SparkBuildProperty {
    override val name: String                 = "spark.app.name"
    override val defaultValue: Option[String] = None
  }

  final case class SchedulerMode(
    override val value: Option[String]
  ) extends SparkBuildProperty {
    override val name: String                 = "spark.scheduler.mode"
    override val defaultValue: Option[String] = Some("FIFO")

    override def validate(propertyValue: String): Unit = {
      val validModes = Set("FIFO", "FAIR")
      if (!validModes.contains(propertyValue)) {
        throw new IllegalArgumentException(
          s"Invalid scheduler mode: $propertyValue. Must be one of: ${validModes.mkString(", ")}"
        )
      }
    }
  }

  final case class SchedulerAllocationFile(
    override val value: Option[String]
  ) extends SparkBuildProperty {
    override val name: String                 = "spark.scheduler.allocation.file"
    override val defaultValue: Option[String] = None

    override def validate(propertyValue: String): Unit =
      if (propertyValue.trim.isEmpty) {
        throw new IllegalArgumentException(
          s"Scheduler allocation file path cannot be empty"
        )
      }
  }

  final case class Serializer(
    override val value: Option[String]
  ) extends SparkBuildProperty {
    private val JAVA_SERIALIZER               = "org.apache.spark.serializer.JavaSerializer"
    private val KRYO_SERIALIZER               = "org.apache.spark.serializer.KryoSerializer"
    override val name: String                 = "spark.serializer"
    override val defaultValue: Option[String] = Some(JAVA_SERIALIZER)

    override def validate(propertyValue: String): Unit = {
      val validSerializers = Set(JAVA_SERIALIZER, KRYO_SERIALIZER)
      if (!validSerializers.contains(propertyValue)) {
        throw new IllegalArgumentException(
          s"Invalid serializer: $propertyValue. Common serializers are: ${validSerializers.mkString(", ")}"
        )
      }
    }
  }

  final case class AnyBuildProperty(
    override val name: String,
    override val value: Option[String],
    override val defaultValue: Option[String]
  ) extends SparkBuildProperty
}
