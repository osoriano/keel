package com.netflix.spinnaker.keel.lifecycle

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.config.LifecycleConfig
import com.netflix.spinnaker.keel.activation.DiscoveryActivated
import com.netflix.spinnaker.keel.telemetry.LifecycleMonitorLoadFailed
import com.netflix.spinnaker.keel.telemetry.LifecycleMonitorTimedOut
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeout
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.core.env.Environment
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.CoroutineContext

@Component
@EnableConfigurationProperties(LifecycleConfig::class)
class LifecycleMonitorScheduler(
  val monitors: List<LifecycleMonitor>,
  val monitorRepository: LifecycleMonitorRepository,
  val publisher: ApplicationEventPublisher,
  val lifecycleConfig: LifecycleConfig,
  private val clock: Clock,
  val spectator: Registry,
  val springEnv: Environment
) : DiscoveryActivated(), CoroutineScope {
  override val coroutineContext: CoroutineContext = Dispatchers.IO

  private val timerBuilder = spectator.createId("keel.scheduled.method.duration")

  private val checkMinAge: Duration
    get() = springEnv.getProperty("keel.lifecycle-monitor.min-age-duration", Duration::class.java, lifecycleConfig.minAgeDuration)

  private val batchSize: Int
    get() = springEnv.getProperty("keel.lifecycle-monitor.batch-size", Int::class.java, lifecycleConfig.batchSize)

  /**
   * Listens for an event with monitor == true that a subclass can handle, and saves that into
   * the database for monitoring.
   */
  @EventListener(StartMonitoringEvent::class)
  fun onStartMonitoringEvent(event: StartMonitoringEvent) {
    log.debug("Saving monitor for event $event")
    monitorRepository.save(event)
  }

  @Scheduled(fixedDelayString = "\${keel.lifecycle-monitor.frequency:PT1S}")
  fun invokeMonitoring() {
    if (enabled.get()) {
      val startTime = clock.instant()
      val job: Job = launch {
        supervisorScope {
          runCatching {
            monitorRepository
              .tasksDueForCheck(checkMinAge, batchSize)
          }
            .onFailure {
              publisher.publishEvent(LifecycleMonitorLoadFailed(it))
            }
            .onSuccess { tasks ->
              tasks.forEach { task ->
                try {
                  /**
                   * Allow individual monitoring to timeout but catch the `CancellationException`
                   * to prevent the cancellation of all coroutines under [job]
                   */
                  /**
                   * Allow individual monitoring to timeout but catch the `CancellationException`
                   * to prevent the cancellation of all coroutines under [job]
                   */
                  withTimeout(lifecycleConfig.timeoutDuration.toMillis()) {
                    launch {
                      monitors
                        .first { it.handles(task.type) }
                        .monitor(task)
                    }
                  }
                } catch (e: TimeoutCancellationException) {
                  log.error("Timed out monitoring task $task", e)
                  publisher.publishEvent(LifecycleMonitorTimedOut(task.type, task.link, task.triggeringEvent.artifactReference))
                }
              }
            }
        }
      }
      runBlocking { job.join() }
      recordDuration(startTime, "lifecycle")
    }
  }

  private fun recordDuration(startTime : Instant, type: String) =
    timerBuilder
      .withTag("type", type)
      .let { id ->
        spectator.timer(id).record(Duration.between(startTime, clock.instant()))
      }
}
