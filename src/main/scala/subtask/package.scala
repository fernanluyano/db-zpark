package dev.fb.dbzpark

package object subtask {

  sealed trait SubtaskContext { def name: String }

  case class SimpleContext(name: String)                    extends SubtaskContext
  case class GroupingContext(name: String, groupId: String) extends SubtaskContext

}
