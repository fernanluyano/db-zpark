package dev.fb.dbzpark

package object subtask {
  case class SubtaskContext(name: String, groupId: Int)

  case class SubtasksRunnerConfig(
    concurrency: Int,
    failFast: Boolean,
    maxRetries: Int
  )
}
