package dev.fb.dbzpark
package unitycatalog

import unitycatalog.Catalogs.{SYSTEM_CATALOG, UcCatalog}

/**
 * Schema-level abstractions for Unity Catalog.
 *
 * Schemas (also known as databases) are the second level in Unity Catalog's
 * three-level namespace hierarchy (catalog.schema.table). Schemas provide
 * logical groupings for tables, views, functions, and other database objects
 * within a catalog.
 */
object Schemas {

  /**
   * Represents a Unity Catalog schema securable.
   *
   * A schema is a container for tables, views, and other database objects within
   * a catalog. Each schema must belong to exactly one catalog.
   *
   * The fully qualified name of a schema follows the format: `catalog_name.schema_name`
   *
   * @example
   * {{{
   * val schema = INFORMATION_SCHEMA
   * schema.getSimpleName // Returns "information_schema"
   * schema.getFullyQualifiedName // Returns "system.information_schema"
   * schema.catalog // Returns SYSTEM_CATALOG
   * }}}
   */
  trait UcSchema extends Securable {

    /**
     * The parent catalog that contains this schema.
     *
     * @return the UcCatalog instance that owns this schema
     */
    val catalog: UcCatalog

    /**
     * Returns the simple (unqualified) name of this schema.
     *
     * @return the schema name without the catalog prefix
     */
    override def getSimpleName: String

    /**
     * Returns the fully qualified name of this schema.
     *
     * The fully qualified name includes the parent catalog name followed by
     * the schema name, separated by a dot.
     *
     * @return the fully qualified schema name in the format "catalog.schema"
     */
    final override def getFullyQualifiedName: String =
      s"${catalog.getSimpleName}.$getSimpleName"
  }

  /**
   * The information schema containing Unity Catalog metadata views.
   *
   * The information_schema is a special built-in schema within the system catalog
   * that provides standardized, SQL-queryable views of Unity Catalog metadata.
   * It contains views for tables, columns, schemas, catalogs, and other metadata
   * following ANSI SQL standard conventions.
   *
   * This schema is read-only and automatically maintained by Unity Catalog.
   *
   * Common views in information_schema include:
   * - `tables`: Metadata about all tables and views
   * - `columns`: Metadata about all columns in tables and views
   * - `schemata`: Metadata about all schemas
   * - `catalogs`: Metadata about all catalogs
   */
  case object INFORMATION_SCHEMA extends UcSchema {

    /**
     * The system catalog that contains the information_schema.
     *
     * @return SYSTEM_CATALOG
     */
    override val catalog: UcCatalog = SYSTEM_CATALOG

    /**
     * Returns "information_schema" as the name of this schema.
     *
     * @return the string "information_schema"
     */
    override def getSimpleName: String = "information_schema"
  }
}
