package com.netflix.spinnaker.keel.orca

import com.netflix.spinnaker.keel.api.actuation.SubjectType.RESOURCE
import com.netflix.spinnaker.keel.api.actuation.Task
import com.netflix.spinnaker.keel.core.api.DEFAULT_SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.events.ResourceTaskFailed
import com.netflix.spinnaker.keel.events.ResourceTaskSucceeded
import com.netflix.spinnaker.keel.events.TaskCreatedEvent
import com.netflix.spinnaker.keel.lifecycle.LifecycleMonitor
import com.netflix.spinnaker.keel.persistence.NoSuchResourceId
import com.netflix.spinnaker.keel.persistence.ResourceRepository
import com.netflix.spinnaker.keel.persistence.TaskRecord
import com.netflix.spinnaker.keel.persistence.TaskTrackingRepository
import com.netflix.spinnaker.keel.retrofit.isNotFound
import com.netflix.spinnaker.keel.scheduled.ScheduledAgent
import com.netflix.spinnaker.keel.scheduled.TaskActuator
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.orca.ExecutionDetailResponse
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import retrofit2.HttpException
import java.time.Clock
import java.util.concurrent.TimeUnit.MINUTES

@Component
class OrcaTaskActuator(
  private val taskTrackingRepository: TaskTrackingRepository,
  private val orcaService: OrcaService,
  private val resourceRepository: ResourceRepository,
  private val publisher: ApplicationEventPublisher,
  private val clock: Clock
) : TaskActuator {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  private val mapper = configuredObjectMapper()

  override suspend fun checkTask(task: TaskRecord) {
    val taskDetails = getTaskExecution(task)
    if (taskDetails == null) {
      taskTrackingRepository.delete(task.id)
      return
    }

    if (!taskDetails.status.isComplete()) {
      return
    }

    // todo switch from events to ensure runs sync
    // only resource events are currently supported
    if (task.subjectType == RESOURCE) {
      val id = checkNotNull(task.resourceId)
      try {
        when (taskDetails.status.isSuccess()) {
          true -> publisher.publishEvent(
            ResourceTaskSucceeded(
              resourceRepository.get(id), listOf(Task(taskDetails.id, taskDetails.name)), clock
            )
          )
          false -> publisher.publishEvent(
            ResourceTaskFailed(
              resourceRepository.get(id),
              taskDetails.execution?.stages.getFailureMessage(mapper)
                ?: "",
              listOf(Task(taskDetails.id, taskDetails.name)), clock
            )
          )
        }
      } catch (e: NoSuchResourceId) {
        log.warn("No resource found for id $task")
      }
    }
    taskTrackingRepository.delete(task.id)

  }

  /* Return the execution for the given task, or null if not found */
  private suspend fun getTaskExecution(task: TaskRecord): ExecutionDetailResponse? {
    try {
      return orcaService.getOrchestrationExecution(task.id, DEFAULT_SERVICE_ACCOUNT)
    } catch (e: HttpException) {
      if (e.isNotFound) {
        log.warn("Exception ${e.message} while fetching execution ${task.id} " +
          "Possible reason: orca has deleted old task ids")
        return null
      }
      throw e
    }
  }
}
