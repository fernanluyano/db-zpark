package dev.fb.dbzpark
package spark

trait SparkProperty {
  protected val name: String
  protected val value: Option[String]
  protected val defaultValue: Option[String]

  def getName: String = name

  def getValue: String = {
    val theValue = (value, defaultValue) match {
      case (None, Some(_default)) => _default
      case (Some(_value), None)   => _value
      case (None, None) =>
        throw new IllegalArgumentException(s"Property $name must either have a value or a default")
    }

    validate(theValue)

    theValue
  }

  def validate(propertyValue: String): Unit = ()
}
