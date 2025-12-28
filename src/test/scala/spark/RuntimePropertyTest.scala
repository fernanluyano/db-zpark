package dev.fb.dbzpark
package spark

import spark.RuntimeProperty._

import org.scalatest.funsuite.AnyFunSuiteLike

class RuntimePropertyTest extends AnyFunSuiteLike {

  // MaxFilesPerTrigger tests
  test("MaxFilesPerTrigger should use provided value when set") {
    val maxFiles = MaxFilesPerTrigger(Some("500"))
    assert(maxFiles.getName == "maxFilesPerTrigger")
    assert(maxFiles.getValue == "500")
  }

  test("MaxFilesPerTrigger should use default 1000 when no value provided") {
    val maxFiles = MaxFilesPerTrigger(None)
    assert(maxFiles.getValue == "1000")
  }

  test("MaxFilesPerTrigger should accept string numbers") {
    val maxFiles = MaxFilesPerTrigger(Some("2000"))
    assert(maxFiles.getValue == "2000")
  }

  // MaxBytesPerTrigger tests
  test("MaxBytesPerTrigger should use provided value when set") {
    val maxBytes = MaxBytesPerTrigger(Some("2g"))
    assert(maxBytes.getName == "maxBytesPerTrigger")
    assert(maxBytes.getValue == "2g")
  }

  test("MaxBytesPerTrigger should use default 1g when no value provided") {
    val maxBytes = MaxBytesPerTrigger(None)
    assert(maxBytes.getValue == "1g")
  }

  // AnyRuntimeProperty tests
  test("AnyRuntimeProperty should use provided value when set") {
    val anyProp = AnyRuntimeProperty(
      "cloudFiles.format",
      Some("json"),
      None
    )
    assert(anyProp.getName == "cloudFiles.format")
    assert(anyProp.getValue == "json")
  }

  test("AnyRuntimeProperty should use default when no value provided") {
    val anyProp = AnyRuntimeProperty(
      "cloudFiles.inferColumnTypes",
      None,
      Some("true")
    )
    assert(anyProp.getValue == "true")
  }

  test("AnyRuntimeProperty should prefer value over default when both provided") {
    val anyProp = AnyRuntimeProperty(
      "cloudFiles.format",
      Some("parquet"),
      Some("json")
    )
    assert(anyProp.getValue == "parquet")
  }

  test("AnyRuntimeProperty should fail when neither value nor default is provided") {
    val exception = intercept[IllegalArgumentException] {
      AnyRuntimeProperty("some.property", None, None).getValue
    }
    assert(exception.getMessage.contains("must either have a value or a default"))
  }

  // Edge case: getValue should be idempotent
  test("getValue should return same value on multiple calls") {
    val maxFiles   = MaxFilesPerTrigger(Some("750"))
    val firstCall  = maxFiles.getValue
    val secondCall = maxFiles.getValue
    assert(firstCall == secondCall)
    assert(firstCall == "750")
  }

  test("Runtime properties with defaults should be constructible and getValue should never throw") {
    val maxFiles = MaxFilesPerTrigger(None)
    val maxBytes = MaxBytesPerTrigger(None)

    // If we got here, construction succeeded
    // getValue should be safe to call multiple times
    assert(maxFiles.getValue == "1000")
    assert(maxFiles.getValue == "1000")
    assert(maxBytes.getValue == "1g")
    assert(maxBytes.getValue == "1g")
  }
}
