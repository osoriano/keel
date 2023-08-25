package com.netflix.spinnaker.keel.scheduled

import com.netflix.spinnaker.keel.persistence.TaskRecord

interface TaskActuator {

  suspend fun checkTask(task: TaskRecord)
}
