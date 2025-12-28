package dev.fb.dbzpark
package spark

/**
 * Build-time Spark properties that must be set before SparkSession initialization.
 * These properties configure fundamental session behavior and cannot be changed at runtime.
 */
object BuildProperty {

  sealed trait SparkBuildProperty extends SparkProperty

  /**
   * Defines the cluster manager connection (local, yarn, mesos, k8s).
   *
   * No default because the deployment target is context-dependent and should be
   * explicitly specified to prevent accidentally running in the wrong mode.
   */
  final case class Master(
    override val value: Option[String]
  ) extends SparkBuildProperty {
    override val name                         = "spark.master"
    override val defaultValue: Option[String] = None
  }

  /**
   * Application name shown in Spark UI and logs.
   *
   * No default to force meaningful naming - generic names make debugging and
   * monitoring across multiple jobs difficult.
   */
  final case class AppName(
    override val value: Option[String]
  ) extends SparkBuildProperty {
    override val name: String                 = PropertyName.APP_NAME
    override val defaultValue: Option[String] = None
  }

  /**
   * FIFO scheduler mode for predictable, simple task ordering within a job.
   */
  case object SchedulerModeFIFO extends SparkBuildProperty {
    override val name: String                 = PropertyName.SCHEDULER_MODE
    override val value: Option[String]        = Some("FIFO")
    override val defaultValue: Option[String] = None
  }

  /**
   * FAIR scheduler mode for equal resource sharing across concurrent jobs.
   */
  case object SchedulerModeFAIR extends SparkBuildProperty {
    override val name: String                 = PropertyName.SCHEDULER_MODE
    override val value: Option[String]        = Some("FAIR")
    override val defaultValue: Option[String] = None
  }

  /**
   * XML file path for FAIR scheduler pool configuration.
   *
   * Only meaningful when SchedulerMode is FAIR. No default because the file location
   * is environment-specific. Validation prevents empty paths that would cause obscure
   * Spark initialization errors.
   */
  final case class SchedulerAllocationFile(
    override val value: Option[String]
  ) extends SparkBuildProperty {
    override val name: String                 = PropertyName.SCHEDULER_ALLOCATION_FILE
    override val defaultValue: Option[String] = None

    override def validate(propertyValue: String): Unit =
      if (propertyValue.trim.isEmpty) {
        throw new IllegalArgumentException(
          s"Scheduler allocation file path cannot be empty"
        )
      }
  }

  /**
   * Serialization framework for shuffling data and caching.
   *
   * Defaults to JavaSerializer for compatibility, but Kryo is often 10x faster and more compact.
   * Validation catches typos in fully-qualified class names that would cause runtime failures
   * deep in job execution.
   */
  final case class Serializer(
    override val value: Option[String]
  ) extends SparkBuildProperty {
    private val JAVA_SERIALIZER               = "org.apache.spark.serializer.JavaSerializer"
    private val KRYO_SERIALIZER               = "org.apache.spark.serializer.KryoSerializer"
    override val name: String                 = PropertyName.SERIALIZER
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

  /**
   * Escape hatch for arbitrary build properties not explicitly modeled.
   *
   * Allows setting any Spark configuration without requiring code changes for every
   * new Spark version or obscure property. Use sparingly - prefer typed properties
   * for better compile-time safety and discoverability.
   */
  final case class AnyBuildProperty(
    override val name: String,
    override val value: Option[String],
    override val defaultValue: Option[String]
  ) extends SparkBuildProperty
}
