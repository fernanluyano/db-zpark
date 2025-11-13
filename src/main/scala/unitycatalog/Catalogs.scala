package dev.fb.dbzpark
package unitycatalog

/**
 * Catalog-level abstractions for Unity Catalog.
 *
 * Catalogs are the top-level namespace in Unity Catalog's three-level hierarchy
 * (catalog.schema.table). They provide logical groupings for schemas and serve
 * as the primary boundary for data organization and access control.
 */
object Catalogs {

  /**
   * Represents a Unity-Catalog catalog securable.
   *
   * A catalog is the top-level container in the Unity Catalog namespace hierarchy.
   * Catalogs contain schemas, which in turn contain tables, views, and other objects.
   *
   * The fully qualified name of a catalog is simply its simple name, as catalogs
   * do not have parent namespaces.
   *
   * @example
   * {{{
   * val catalog = SYSTEM_CATALOG
   * catalog.getSimpleName // Returns "system"
   * catalog.getFullyQualifiedName // Returns "system"
   * }}}
   */
  trait UcCatalog extends Securable {

    /**
     * Returns the simple name of this catalog.
     *
     * @return the catalog name
     */
    override def getSimpleName: String

    /**
     * Returns the fully qualified name of this catalog.
     *
     * For catalogs, the fully qualified name is identical to the simple name
     * since they are at the top of the namespace hierarchy.
     *
     * @return the catalog name (same as getSimpleName)
     */
    final override def getFullyQualifiedName: String = getSimpleName
  }

  /**
   * The system catalog containing Unity Catalog metadata tables.
   *
   * The system catalog is a special built-in catalog that contains metadata
   * about the Unity Catalog itself, including information about securables,
   * permissions, and lineage. It includes the `information_schema` which
   * provides SQL-queryable metadata views.
   *
   * This catalog is read-only and managed by the Unity Catalog system.
   */
  case object SYSTEM_CATALOG extends UcCatalog {

    /**
     * Returns "system" as the name of the system catalog.
     *
     * @return the string "system"
     */
    override def getSimpleName: String = "system"
  }
}
