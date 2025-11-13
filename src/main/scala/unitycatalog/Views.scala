package dev.fb.dbzpark
package unitycatalog

import unitycatalog.Schemas.UcSchema

/**
 * View-level abstractions for Unity Catalog.
 *
 * Views are virtual tables defined by SQL queries within Unity Catalog's three-level
 * namespace hierarchy (catalog.schema.view). Views provide a way to present data
 * from one or more tables through a saved query definition, without storing the
 * data itself.
 */
object Views {

  /**
   * Represents a Unity Catalog view securable.
   *
   * A view is a virtual table defined by a SQL query. Unlike tables, views do not
   * store data themselves; instead, they dynamically compute results based on their
   * underlying query definition when accessed.
   *
   * Views can be:
   * - Standard views: Regular views that execute their query each time they're accessed
   * - Materialized views: Views with cached results for improved query performance
   *
   * Each view must belong to exactly one schema, which in turn belongs to a catalog.
   *
   * The fully qualified name of a view follows the format:
   * `catalog_name.schema_name.view_name`
   *
   * @example
   * {{{
   * case object MyView extends UcView {
   *   override val schema: UcSchema = MySchema
   *   override def getSimpleName: String = "my_view"
   * }
   *
   * MyView.getSimpleName // Returns "my_view"
   * MyView.getFullyQualifiedName // Returns "my_catalog.my_schema.my_view"
   * MyView.schema // Returns MySchema
   * }}}
   */
  trait UcView extends Securable {

    /**
     * The parent schema that contains this view.
     *
     * @return the UcSchema instance that owns this view
     */
    val schema: UcSchema

    /**
     * Returns the simple (unqualified) name of this view.
     *
     * @return the view name without the catalog or schema prefix
     */
    override def getSimpleName: String

    /**
     * Returns the fully qualified name of this view.
     *
     * The fully qualified name includes the catalog name, schema name, and view name,
     * all separated by dots.
     *
     * @return the fully qualified view name in the format "catalog.schema.view"
     */
    final override def getFullyQualifiedName: String =
      s"${schema.getFullyQualifiedName}.$getSimpleName"
  }
}
