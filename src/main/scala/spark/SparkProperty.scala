package dev.fb.dbzpark
package spark

/**
 * Base trait for all Spark configuration properties.
 *
 * Encapsulates the common pattern of Spark properties having a name, optional value,
 * and optional default. The design enforces fail-fast behavior - properties are eagerly
 * resolved and validated at construction time (via `resolvedValue` val initialization),
 * ensuring misconfigured properties fail immediately rather than during Spark session
 * creation where errors are harder to diagnose and debug.
 */
trait SparkProperty {
  protected val name: String
  protected val value: Option[String]
  protected val defaultValue: Option[String]

  // Delayed init to make sure the fields in concrete implementations are set so these don't become null
  private lazy val resolvedValue = resolveValue

  def getName: String  = name
  def getValue: String = resolvedValue

  /**
   * Resolves the property to its concrete value using precedence: explicit value > default > error.
   * @return the resolved property value
   * @throws IllegalArgumentException if neither value nor defaultValue is provided, or if validation fails
   */
  private def resolveValue: String = {
    val theValue = (value, defaultValue) match {
      case (None, Some(_default)) => _default
      case (Some(_value), _)      => _value
      case (None, None) =>
        throw new IllegalArgumentException(s"Property $name must either have a value or a default")
    }

    validate(theValue)

    theValue
  }

  /**
   * Validates the property value against domain-specific constraints.
   *
   * Called during construction (via resolveValue) to ensure all validation errors surface
   * immediately when the property object is created. Default implementation is a no-op to
   * avoid boilerplate for properties without constraints. Override this method only when
   * domain validation is needed (e.g., enum values, file path existence, numeric ranges).
   *
   * @param propertyValue the value to validate
   * @throws IllegalArgumentException if validation fails
   */
  def validate(propertyValue: String): Unit = ()
}
