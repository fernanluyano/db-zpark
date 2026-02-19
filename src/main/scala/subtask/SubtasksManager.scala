package dev.fb.dbzpark
package subtask

import zio.Schedule
import zio.ZIO
import zio.durationInt
import zio.RIO
import zio.Task

final class SubtasksManager private (private val environment: TaskEnvironment) {

  def run: RIO[TaskEnvironment, Unit] = {
    val schedule = Schedule.recurWhile[Unit](_ => !environment.subtasksGraph.isEmpty) && Schedule.fixed(1.second)

    dispatcher.repeat(schedule).unit
  }

  private def dispatcher: RIO[TaskEnvironment, Unit] =
    ZIO
      .succeed(environment.subtasksGraph.getZeroInDegree)
      .map(tasks => tasks.sortBy(_.subtask.localPriority)(Ordering.Int.reverse))
      .flatMap(nextTasks =>
        ZIO
          .foreachParDiscard(nextTasks)(runAndManage)
          .onExecutor(environment.subtasksExecutor)
          .withParallelism(environment.maxConcurrentSubtasks)
      )

  private def runAndManage(subtaskNode: SubtaskNode): RIO[TaskEnvironment, Unit] =
    for {
      _ <- ZIO.logInfo(s"Task scheduled for execution: ${subtaskNode.subtask}")
      _ <- subtaskNode.subtask.run.foldZIO(
             failure = e => ZIO.logError("SubtaskNode failed\n" + e.getMessage()) *> handleNodeFailure(subtaskNode),
             success = _ => handleNodeSuccess(subtaskNode)
           )
    } yield ()

  private def handleNodeFailure(subtaskNode: SubtaskNode): Task[Unit] =
    ZIO.attempt(environment.subtasksGraph.finalizeNode(subtaskNode, FAILED))

  private def handleNodeSuccess(subtaskNode: SubtaskNode): Task[Unit] =
    ZIO.attempt(environment.subtasksGraph.finalizeNode(subtaskNode, SUCCEEDED))
}

object SubtasksManager {
  def apply(environment: TaskEnvironment): SubtasksManager =
    new SubtasksManager(environment)
}
