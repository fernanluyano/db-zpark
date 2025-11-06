package dev.fb.dbzpark

package object subtask {

  /**
   * Context information for a subtask execution.
   *
   * This case class encapsulates metadata about a subtask, primarily used for
   * logging and tracking purposes within a workflow. Each subtask in a workflow
   * should have a unique context to identify it during execution.
   *
   * @param name The unique identifier or descriptive name of the subtask.
   *             This name is used in logs and metrics to track subtask execution.
   */
  case class SubtaskContext(name: String)
}
