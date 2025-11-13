package dev.fb.dbzpark

/**
 * Unity Catalog package providing abstractions for Databricks Unity Catalog securables.
 *
 * This package defines the core traits and objects representing Unity Catalog entities
 * such as catalogs, schemas, tables, and views, following the three-level namespace
 * hierarchy: catalog.schema.object.
 */
package object unitycatalog {

  /**
   * Base trait for all Unity Catalog securable objects.
   *
   * A securable is any object in Unity Catalog that can have permissions applied to it.
   * This includes catalogs, schemas, tables, views, functions, and other database objects.
   *
   * Securables in Unity Catalog follow a hierarchical naming structure:
   * - Catalogs: `catalog_name`
   * - Schemas: `catalog_name.schema_name`
   * - Tables/Views: `catalog_name.schema_name.table_name`
   */
  trait Securable {

    /**
     * Returns the simple (unqualified) name of this securable.
     *
     * @return the simple name without any parent namespace qualifiers
     * @example For a table named "my_table" in schema "my_schema" in catalog "my_catalog",
     *          this returns "my_table"
     */
    def getSimpleName: String

    /**
     * Returns the fully qualified name of this securable.
     *
     * The fully qualified name includes all parent namespaces in the hierarchy.
     *
     * @return the complete path to this securable in dot notation
     * @example For a table named "my_table" in schema "my_schema" in catalog "my_catalog",
     *          this returns "my_catalog.my_schema.my_table"
     */
    def getFullyQualifiedName: String
  }
}
