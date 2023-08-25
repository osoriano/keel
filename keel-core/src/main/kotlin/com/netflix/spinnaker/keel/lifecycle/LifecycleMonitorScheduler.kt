package com.netflix.spinnaker.keel.lifecycle

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Timer
import com.netflix.spectator.api.histogram.PercentileTimer
import com.netflix.spinnaker.config.LifecycleConfig
import com.netflix.spinnaker.keel.activation.ApplicationDown
import com.netflix.spinnaker.keel.activation.ApplicationUp
import com.netflix.spinnaker.keel.telemetry.recordDurationPercentile
import com.netflix.spinnaker.keel.telemetry.safeIncrement
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

@Component
@EnableConfigurationProperties(LifecycleConfig::class)
class LifecycleMonitorScheduler(
  val monitors: List<LifecycleMonitor>,
  val monitorRepository: LifecycleMonitorRepository,
  val publisher: ApplicationEventPublisher,
  val lifecycleConfig: LifecycleConfig,
  private val clock: Clock,
  val spectator: Registry
) : CoroutineScope {
  override val coroutineContext: CoroutineContext = Dispatchers.IO

  private val enabled = AtomicBoolean(false)

  @EventListener(ApplicationUp::class)
  fun onApplicationUp() {
    log.info("Application up, enabling scheduled lifecycle monitoring")
    enabled.set(true)
  }

  @EventListener(ApplicationDown::class)
  fun onApplicationDown() {
    log.info("Application down, disabling scheduled lifecycle monitoring")
    enabled.set(false)
  }

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

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
              .tasksDueForCheck(lifecycleConfig.minAgeDuration, lifecycleConfig.batchSize)
          }
            .onFailure {
              log.error("Exception fetching monitor tasks due for check", it)
            }
            .onSuccess {
              it.forEach {
                launch {
                  try {
                    withTimeout(lifecycleConfig.timeoutDuration.toMillis()) {
                      val lifecycleMonitor = monitors.supporting(it.type)
                      lifecycleMonitor.monitor(it)
                    }
                  } catch (e: TimeoutCancellationException) {
                    log.error("Timed out monitoring task $it", e)
                    spectator.counter(
                      "keel.scheduled.timeout",
                      listOf(BasicTag("type",  "lifecycle"))
                    ).safeIncrement()
                  } catch (e: Exception) {
                    log.error("Failed monitoring task $it", e)
                    spectator.counter(
                      "keel.scheduled.failure",
                      listOf(BasicTag("type",  "lifecycle"))
                    ).safeIncrement()
                  }
                }
              }
              spectator.counter(
                "keel.scheduled.batch.size",
                listOf(BasicTag("type", "lifecycle"))
              ).increment(it.size.toLong())
            }
        }
      }
      runBlocking { job.join() }
      recordDuration(startTime, "lifecycle")
    }
  }

  private fun recordDuration(startTime: Instant, type: String) =
    spectator.recordDurationPercentile("keel.scheduled.method.duration", clock, startTime, setOf(BasicTag("type", type)))
}
