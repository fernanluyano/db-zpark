package dev.fb.dbzpark
package unitycatalog

import unitycatalog.Catalogs.UcCatalog
import unitycatalog.Schemas.UcSchema
import unitycatalog.Tables.UcTable

import org.scalatest.funsuite.AnyFunSuiteLike

class TablesTest extends AnyFunSuiteLike {
  test("tableNameTest") {
    val devCatalog = new UcCatalog {
      override def getSimpleName: String = "dev"
    }
    val sfSchema = new UcSchema {
      override val catalog: Catalogs.UcCatalog = devCatalog

      override def getSimpleName: String = "salesforce"
    }
    val table = new UcTable {
      override val schema: Schemas.UcSchema = sfSchema

      override def getSimpleName: String = "table_1"
    }

    assertResult("table_1")(table.getSimpleName)
    assertResult("dev.salesforce.table_1")(table.getFullyQualifiedName)
  }
}
