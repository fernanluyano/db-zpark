package dev.fb.dbzpark
package example

import unitycatalog.Catalogs
import unitycatalog.Catalogs.UcCatalog
import unitycatalog.Schemas.UcSchema
import unitycatalog.Tables.UcTable

/**
 * Package object defining Unity Catalog table references for the simple example.
 *
 * This demonstrates how to structure catalog/schema/table metadata, but is specific to this example.
 * Your subtasks can work with any data sources - Unity Catalog tables, files, APIs, databases, etc.
 */
package object simple {
  private val devCatalog = new UcCatalog {
    override def getSimpleName: String = "dev"
  }

  private val salesforceBronzeSchema = new UcSchema {
    override val catalog: Catalogs.UcCatalog = devCatalog
    override def getSimpleName: String       = "salesforce_bronze"
  }

  private val salesforceSilverSchema = new UcSchema {
    override val catalog: Catalogs.UcCatalog = devCatalog
    override def getSimpleName: String       = "salesforce_silver"
  }

  /** Base trait for tables in the salesforce_bronze schema */
  trait SalesforceBronzeTable extends UcTable {
    override val schema: UcSchema = salesforceBronzeSchema
  }

  /** Base trait for tables in the salesforce_silver schema */
  trait SalesforceSilverTable extends UcTable {
    override val schema: UcSchema = salesforceSilverSchema
  }

  /** Bronze layer Salesforce Account table (dev.salesforce_bronze.account) */
  case object SfAccountBronze extends SalesforceBronzeTable {
    override def getSimpleName: String = "account"
  }

  /** Silver layer Salesforce Account table (dev.salesforce_silver.account) */
  case object SfAccountSilver extends SalesforceSilverTable {
    override def getSimpleName: String = "account"
  }
}
