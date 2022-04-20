package com.netflix.spinnaker.keel.scheduling.activities

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.actuation.ResourceActuator
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.scheduling.TEMPORAL_CHECKER
import com.netflix.spinnaker.keel.telemetry.ResourceCheckCompleted
import com.netflix.spinnaker.keel.telemetry.ResourceCheckStarted
import com.netflix.spinnaker.keel.telemetry.ResourceLoadFailed
import com.netflix.spinnaker.keel.telemetry.recordDurationPercentile
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
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
  private val spectator: Registry
) : ActuatorActivities {

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
          }
        }
    }
    recordDuration(startTime, "resource")
  }

  private fun recordDuration(startTime : Instant, type: String) =
    spectator.recordDurationPercentile("keel.scheduled.method.duration", startTime, clock.instant(), setOf(BasicTag("type", type), BasicTag("checker", TEMPORAL_CHECKER)))
}
