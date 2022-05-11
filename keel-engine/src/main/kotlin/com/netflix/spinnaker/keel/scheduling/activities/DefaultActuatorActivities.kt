package com.netflix.spinnaker.keel.scheduling.activities

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.actuation.EnvironmentPromotionChecker
import com.netflix.spinnaker.keel.actuation.ResourceActuator
import com.netflix.spinnaker.keel.actuation.ScheduledEnvironmentCheckStarting
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.TEMPORAL_CHECKER
import com.netflix.spinnaker.keel.telemetry.EnvironmentCheckComplete
import com.netflix.spinnaker.keel.telemetry.EnvironmentCheckStarted
import com.netflix.spinnaker.keel.telemetry.ResourceCheckCompleted
import com.netflix.spinnaker.keel.telemetry.ResourceCheckStarted
import com.netflix.spinnaker.keel.telemetry.ResourceLoadFailed
import com.netflix.spinnaker.keel.telemetry.recordDurationPercentile
import io.temporal.failure.ApplicationFailure
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
  private val spectator: Registry,
  private val environmentPromotionChecker: EnvironmentPromotionChecker
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

            keelRepository.getLastCheckedTime(it)?.let { lastCheckTime ->
              recordCheckAge(lastCheckTime, "resource")
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

  override fun checkEnvironment(request: ActuatorActivities.CheckEnvironmentRequest) {
    val startTime = clock.instant()
    publisher.publishEvent(ScheduledEnvironmentCheckStarting)
    publisher.publishEvent(EnvironmentCheckStarted(request.application, TEMPORAL_CHECKER))

    runBlocking {
      environmentPromotionChecker.checkEnvironment(request.application, request.environment)
      publisher.publishEvent(EnvironmentCheckComplete(
        application = request.application,
        duration = Duration.between(startTime, clock.instant()),
        checker = TEMPORAL_CHECKER
      ))

      keelRepository.getEnvLastCheckedTime(request.application, request.environment)?.let { lastCheckTime ->
        recordCheckAge(lastCheckTime, "environment")
      }
      keelRepository.setEnvLastCheckedTime(request.application, request.environment)
    }
    recordDuration(startTime, "environment")
  }

  override fun monitorEnvironment(request: ActuatorActivities.MonitorEnvironmentRequest) {
    checkEnvironment(request.toCheckEnvironmentRequest())
    throw ApplicationFailure.newFailure("continuation", "expected")
  }

  private fun recordDuration(startTime: Instant, type: String) =
    spectator.recordDurationPercentile("keel.scheduled.method.duration", startTime, clock.instant(), setOf(BasicTag("type", type), BasicTag("checker", TEMPORAL_CHECKER)))

  private fun recordCheckAge(lastCheck: Instant, type: String) =
    spectator.recordDurationPercentile("keel.periodically.checked.age", lastCheck, clock.instant(), setOf(BasicTag("type", type), BasicTag("scheduler", "temporal")))
}
