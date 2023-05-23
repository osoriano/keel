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
import com.netflix.spinnaker.keel.persistence.TaskTrackingRepository
import com.netflix.spinnaker.keel.retrofit.isNotFound
import com.netflix.spinnaker.keel.scheduled.ScheduledAgent
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
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

/**
 * This class monitors all tasks in flight.
 *
 * todo eb: Convert to [LifecycleMonitor] to take advantage of batch sizing and the same
 *  way of distributing checking across instances.
 */
@Component
class OrcaTaskMonitorAgent(
  private val taskTrackingRepository: TaskTrackingRepository,
  private val resourceRepository: ResourceRepository,
  private val orcaService: OrcaService,
  private val publisher: ApplicationEventPublisher,
  private val clock: Clock
) : ScheduledAgent {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  override val lockTimeoutSeconds = MINUTES.toSeconds(1)

  private val mapper = configuredObjectMapper()

  @EventListener(TaskCreatedEvent::class)
  fun onTaskEvent(event: TaskCreatedEvent) {
    GlobalScope.launch(Dispatchers.IO) {
      taskTrackingRepository.store(event.taskRecord)
    }
  }

  override suspend fun invokeAgent() {
    log.info("Agent is now a noop")
  }
}
