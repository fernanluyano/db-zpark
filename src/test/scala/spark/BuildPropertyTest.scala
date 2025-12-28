package dev.fb.dbzpark
package spark

import org.scalatest.funsuite.AnyFunSuiteLike
import BuildProperty._

class BuildPropertyTest extends AnyFunSuiteLike {

  // Master tests
  test("Master should use provided value when set") {
    val master = Master(Some("local[*]"))
    assert(master.getName == "spark.master")
    assert(master.getValue == "local[*]")
  }

  test("Master should fail when neither value nor default is provided") {
    val exception = intercept[IllegalArgumentException] {
      Master(None).getValue
    }
    assert(exception.getMessage.contains("must either have a value or a default"))
  }

  // AppName tests
  test("AppName should use provided value when set") {
    val appName = AppName(Some("MySparkApp"))
    assert(appName.getName == "spark.app.name")
    assert(appName.getValue == "MySparkApp")
  }

  test("AppName should fail when neither value nor default is provided") {
    val exception = intercept[IllegalArgumentException] {
      AppName(None).getValue
    }
    assert(exception.getMessage.contains("must either have a value or a default"))
  }

  // SchedulerMode tests
  test("SchedulerMode should use provided value when valid") {
    val schedulerMode = SchedulerMode(Some("FAIR"))
    assert(schedulerMode.getName == "spark.scheduler.mode")
    assert(schedulerMode.getValue == "FAIR")
  }

  test("SchedulerMode should use default FIFO when no value provided") {
    val schedulerMode = SchedulerMode(None)
    assert(schedulerMode.getValue == "FIFO")
  }

  test("SchedulerMode should fail with invalid mode at construction time") {
    val exception = intercept[IllegalArgumentException] {
      SchedulerMode(Some("INVALID")).getValue
    }
    assert(exception.getMessage.contains("Invalid scheduler mode: INVALID"))
    assert(exception.getMessage.contains("Must be one of: FIFO, FAIR"))
  }

  test("SchedulerMode should validate both FIFO and FAIR as valid") {
    assert(SchedulerMode(Some("FIFO")).getValue == "FIFO")
    assert(SchedulerMode(Some("FAIR")).getValue == "FAIR")
  }

  // SchedulerAllocationFile tests
  test("SchedulerAllocationFile should use provided value when set") {
    val allocationFile = SchedulerAllocationFile(Some("/path/to/fairscheduler.xml"))
    assert(allocationFile.getName == "spark.scheduler.allocation.file")
    assert(allocationFile.getValue == "/path/to/fairscheduler.xml")
  }

  test("SchedulerAllocationFile should fail when value is empty string at construction time") {
    val exception = intercept[IllegalArgumentException] {
      SchedulerAllocationFile(Some("")).getValue
    }
    assert(exception.getMessage.contains("Scheduler allocation file path cannot be empty"))
  }

  test("SchedulerAllocationFile should fail when value is only whitespace at construction time") {
    val exception = intercept[IllegalArgumentException] {
      SchedulerAllocationFile(Some("   ")).getValue
    }
    assert(exception.getMessage.contains("Scheduler allocation file path cannot be empty"))
  }

  test("SchedulerAllocationFile should fail when neither value nor default is provided") {
    val exception = intercept[IllegalArgumentException] {
      SchedulerAllocationFile(None).getValue
    }
    assert(exception.getMessage.contains("must either have a value or a default"))
  }

  // Serializer tests
  test("Serializer should use JavaSerializer default when no value provided") {
    val serializer = Serializer(None)
    assert(serializer.getValue == "org.apache.spark.serializer.JavaSerializer")
  }

  test("Serializer should use provided KryoSerializer when set") {
    val serializer = Serializer(Some("org.apache.spark.serializer.KryoSerializer"))
    assert(serializer.getName == "spark.serializer")
    assert(serializer.getValue == "org.apache.spark.serializer.KryoSerializer")
  }

  test("Serializer should fail with invalid serializer at construction time") {
    val exception = intercept[IllegalArgumentException] {
      Serializer(Some("com.invalid.CustomSerializer")).getValue
    }
    assert(exception.getMessage.contains("Invalid serializer: com.invalid.CustomSerializer"))
    assert(exception.getMessage.contains("Common serializers are:"))
  }

  test("Serializer should validate both Java and Kryo serializers") {
    assert(
      Serializer(Some("org.apache.spark.serializer.JavaSerializer")).getValue ==
        "org.apache.spark.serializer.JavaSerializer"
    )
    assert(
      Serializer(Some("org.apache.spark.serializer.KryoSerializer")).getValue ==
        "org.apache.spark.serializer.KryoSerializer"
    )
  }

  // AnyBuildProperty tests
  test("AnyBuildProperty should use provided value when set") {
    val anyProp = AnyBuildProperty(
      "spark.custom.property",
      Some("customValue"),
      None
    )
    assert(anyProp.getName == "spark.custom.property")
    assert(anyProp.getValue == "customValue")
  }

  test("AnyBuildProperty should use default when no value provided") {
    val anyProp = AnyBuildProperty(
      "spark.custom.property",
      None,
      Some("defaultValue")
    )
    assert(anyProp.getValue == "defaultValue")
  }

  test("AnyBuildProperty should prefer value over default when both provided") {
    val anyProp = AnyBuildProperty(
      "spark.custom.property",
      Some("explicitValue"),
      Some("defaultValue")
    )
    assert(anyProp.getValue == "explicitValue")
  }

  test("AnyBuildProperty should fail when neither value nor default is provided") {
    val exception = intercept[IllegalArgumentException] {
      AnyBuildProperty("spark.custom.property", None, None).getValue
    }
    assert(exception.getMessage.contains("must either have a value or a default"))
  }

  // Edge case: getValue should be idempotent (testing eager resolution)
  test("getValue should return same value on multiple calls without re-validation") {
    val schedulerMode = SchedulerMode(Some("FAIR"))
    val firstCall     = schedulerMode.getValue
    val secondCall    = schedulerMode.getValue
    assert(firstCall == secondCall)
    assert(firstCall == "FAIR")
  }

  // Edge case: Construction time validation means getValue never throws after object creation
  test("Properties with defaults should be constructible and getValue should never throw") {
    val serializer = Serializer(None)
    // If we got here, construction succeeded with validation
    // getValue should now be safe to call multiple times
    assert(serializer.getValue == "org.apache.spark.serializer.JavaSerializer")
    assert(serializer.getValue == "org.apache.spark.serializer.JavaSerializer")
  }
}
