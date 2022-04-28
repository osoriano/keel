package com.netflix.spinnaker.keel.scheduling.activities

import com.google.common.base.Ticker
import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.actuation.ResourceActuator
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.TEMPORAL_CHECKER
import com.netflix.spinnaker.keel.telemetry.ResourceCheckCompleted
import com.netflix.spinnaker.keel.telemetry.ResourceCheckStarted
import com.netflix.spinnaker.keel.telemetry.ResourceLoadFailed
import com.netflix.spinnaker.keel.telemetry.recordDurationPercentile
import io.temporal.activity.Activity
import io.temporal.client.ActivityCompletionException
import io.temporal.failure.ApplicationFailure
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

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
          }
        }
    }
    recordDuration(startTime, "resource")
    request.lastChecked?.also {
      recordCheckAge(it)
    }
  }

  override fun monitorResource(request: ActuatorActivities.MonitorResourceRequest) {
    var lastChecked = request.lastChecked
    while (true) {
      val startTime = System.currentTimeMillis()

      // heartbeat first: this allows any cancellation to abort the check process ASAP without potentially causing
      // a delayed check conflicting with some manual action.
      try {
        Activity.getExecutionContext().heartbeat("polling")
      } catch (e: ActivityCompletionException) {
        throw e
      }

      log.info("Checking resource ${request.resourceKind}/${request.resourceId}")

      checkResource(request.toCheckResourceRequest(lastChecked))

      val resourceKindInterval = configActivities.getResourceKindCheckInterval(SchedulingConfigActivities.CheckResourceKindRequest(request.resourceKind))
      val sleepTime = resourceKindInterval.minus(System.currentTimeMillis() - startTime, ChronoUnit.MILLIS)
      sleep(sleepTime)

      lastChecked = Instant.now()
    }
  }

  private fun sleep(duration: Duration) {
    if (duration.isNegative || duration.isZero) {
      return
    }
    try {
      Thread.sleep(duration.toMillis())
    } catch (e: InterruptedException) {
      // do nothing
    }
  }

  private fun recordDuration(startTime: Instant, type: String) =
    spectator.recordDurationPercentile("keel.scheduled.method.duration", startTime, clock.instant(), setOf(BasicTag("type", type), BasicTag("checker", TEMPORAL_CHECKER)))

  private fun recordCheckAge(lastCheck: Instant) =
    spectator.recordDurationPercentile("keel.periodically.checked.age", lastCheck, clock.instant(), setOf(BasicTag("type", "resource"), BasicTag("scheduler", "temporal")))
}
