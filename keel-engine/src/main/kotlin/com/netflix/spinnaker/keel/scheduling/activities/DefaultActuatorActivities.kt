package com.netflix.spinnaker.keel.scheduling.activities

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.actuation.ResourceActuator
import com.netflix.spinnaker.keel.events.ResourceState
import com.netflix.spinnaker.keel.events.ResourceState.Ok
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.TEMPORAL_CHECKER
import com.netflix.spinnaker.keel.telemetry.ResourceCheckCompleted
import com.netflix.spinnaker.keel.telemetry.ResourceCheckStarted
import com.netflix.spinnaker.keel.telemetry.ResourceLoadFailed
import com.netflix.spinnaker.keel.telemetry.recordDurationPercentile
import io.temporal.failure.ApplicationFailure
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import java.time.Instant

@Component
class DefaultActuatorActivities(
  private val keelRepository: KeelRepository,
  private val resourceActuator: ResourceActuator,
  private val publisher: ApplicationEventPublisher,
  private val clock: Clock,
  private val spectator: Registry,
  private val configActivities: SchedulingConfigActivities
) : ActuatorActivities {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  override fun checkResource(request: ActuatorActivities.CheckResourceRequest) {
    val startTime = clock.instant()
    runBlocking {
      runCatching { keelRepository.getResource(request.resourceId) }
        .onFailure { publisher.publishEvent(ResourceLoadFailed(it)) }
        .onSuccess {
          launch {
            publisher.publishEvent(ResourceCheckStarted(it, TEMPORAL_CHECKER))
            resourceActuator.checkResource(it)
            publisher.publishEvent(ResourceCheckCompleted(Duration.between(startTime, clock.instant()), it.id, TEMPORAL_CHECKER))

            keelRepository.getLastCheckedTime(it)?.let { lastCheckTime ->
              recordCheckAge(lastCheckTime)
            }
            keelRepository.setLastCheckedTime(it)
          }
        }
    }
    recordDuration(startTime, "resource")
  }

  override fun monitorResource(request: ActuatorActivities.MonitorResourceRequest) {
    checkResource(request.toCheckResourceRequest())
    throw ApplicationFailure.newFailure("continuation", "expected")
  }

  private fun recordDuration(startTime: Instant, type: String) =
    spectator.recordDurationPercentile("keel.scheduled.method.duration", startTime, clock.instant(), setOf(BasicTag("type", type), BasicTag("checker", TEMPORAL_CHECKER)))

  private fun recordCheckAge(lastCheck: Instant) =
    spectator.recordDurationPercentile("keel.periodically.checked.age", lastCheck, clock.instant(), setOf(BasicTag("type", "resource"), BasicTag("scheduler", "temporal")))
}
