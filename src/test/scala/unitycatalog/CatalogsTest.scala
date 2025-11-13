package dev.fb.dbzpark
package unitycatalog

import org.scalatest.funsuite.AnyFunSuiteLike

class CatalogsTest extends AnyFunSuiteLike {
  test("systemCatalogTest") {
    val expected = "system"
    assertResult(expected)(Catalogs.SYSTEM_CATALOG.getFullyQualifiedName)
    assertResult(expected)(Catalogs.SYSTEM_CATALOG.getSimpleName)
  }
}
