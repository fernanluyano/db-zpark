package dev.fb.dbzpark
package spark

object RuntimeProperty {

  trait SparkRuntimeProperty extends SparkProperty

  final case class MaxFilesPerTrigger(override val value: Option[String]) extends SparkRuntimeProperty {
    override protected val name: String                 = "maxFilesPerTrigger"
    override protected val defaultValue: Option[String] = Some("1000")
  }

  final case class MaxBytesPerTrigger(override val value: Option[String]) extends SparkRuntimeProperty {
    override protected val name: String                 = "maxBytesPerTrigger"
    override protected val defaultValue: Option[String] = Some("1g")
  }

  final case class AnyRuntimeProperty(
    override val name: String,
    override val value: Option[String],
    override val defaultValue: Option[String]
  ) extends SparkRuntimeProperty
}
