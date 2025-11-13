package dev.fb.dbzpark
package unitycatalog

import unitycatalog.Schemas.UcSchema

/**
 * Table-level abstractions for Unity Catalog.
 *
 * Tables are the third level in Unity Catalog's three-level namespace hierarchy
 * (catalog.schema.table). Tables are the primary objects for storing and organizing
 * structured data within a schema.
 */
object Tables {

  /**
   * Represents a Unity Catalog table securable.
   *
   * A table is a structured data object within a schema. Each table must belong to
   * exactly one schema, which in turn belongs to a catalog.
   *
   * Tables can be:
   * - Managed tables: Data and metadata are both managed by Unity Catalog
   * - External tables: Only metadata is managed; data resides in an external location
   * - Temporary tables: Session-scoped tables that are automatically dropped
   *
   * The fully qualified name of a table follows the format:
   * `catalog_name.schema_name.table_name`
   *
   * @example
   * {{{
   * case object MyTable extends UcTable {
   *   override val schema: UcSchema = MySchema
   *   override def getSimpleName: String = "my_table"
   * }
   *
   * MyTable.getSimpleName // Returns "my_table"
   * MyTable.getFullyQualifiedName // Returns "my_catalog.my_schema.my_table"
   * MyTable.schema // Returns MySchema
   * }}}
   */
  trait UcTable extends Securable {

    /**
     * The parent schema that contains this table.
     *
     * @return the UcSchema instance that owns this table
     */
    val schema: UcSchema

    /**
     * Returns the simple (unqualified) name of this table.
     *
     * @return the table name without the catalog or schema prefix
     */
    override def getSimpleName: String

    /**
     * Returns the fully qualified name of this table.
     *
     * The fully qualified name includes the catalog name, schema name, and table name,
     * all separated by dots.
     *
     * @return the fully qualified table name in the format "catalog.schema.table"
     */
    final override def getFullyQualifiedName: String =
      s"${schema.getFullyQualifiedName}.$getSimpleName"
  }
}
