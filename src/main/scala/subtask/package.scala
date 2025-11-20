package dev.fb.dbzpark

/**
 * Core types and utilities for the subtask workflow framework.
 */
package object subtask {

  /**
   * Metadata context for a subtask.
   */
  sealed trait SubtaskContext {

    /**
     * Unique name identifying this subtask.
     */
    def name: String
  }

  /**
   * Basic context for subtasks without grouping requirements.
   *
   * @param name
   *   Unique identifier for the subtask
   */
  case class SimpleContext(name: String) extends SubtaskContext

  /**
   * Context for subtasks that belong to a logical group.
   *
   * When using GROUP_DEPENDENCIES strategy, subtasks within the same group execute concurrently, while different
   * groups execute sequentially.
   *
   * @param name
   *   Unique identifier for the subtask
   * @param groupId
   *   Identifier for the group this subtask belongs to
   */
  case class GroupingContext(name: String, groupId: String) extends SubtaskContext

}
