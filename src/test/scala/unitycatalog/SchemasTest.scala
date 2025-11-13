package dev.fb.dbzpark
package unitycatalog

import unitycatalog.Schemas.INFORMATION_SCHEMA

import org.scalatest.funsuite.AnyFunSuiteLike

class SchemasTest extends AnyFunSuiteLike {
  test("informationSchemaTest") {
    val schemata = INFORMATION_SCHEMA
    assertResult("information_schema")(schemata.getSimpleName)
    assertResult("system.information_schema")(schemata.getFullyQualifiedName)
  }
}
