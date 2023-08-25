package com.netflix.spinnaker.keel.persistence

import com.netflix.spinnaker.keel.api.TaskStatus
import com.netflix.spinnaker.keel.api.actuation.SubjectType
import java.time.Duration

interface TaskTrackingRepository {
  fun store(task: TaskRecord)
  fun getIncompleteTasks(minTimeSinceLastCheck: Duration, limit: Int): Set<TaskRecord>
  fun delete(taskId: String)
}

data class TaskRecord(
  val id: String,
  val name: String,
  val subjectType: SubjectType,
  val application: String,
  val environmentName: String?,
  val resourceId: String?
)
