package com.netflix.spinnaker.keel.artifacts

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.config.ArtifactConfig
import com.netflix.spinnaker.keel.activation.ApplicationDown
import com.netflix.spinnaker.keel.activation.ApplicationUp
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.events.ArtifactRegisteredEvent
import com.netflix.spinnaker.keel.api.events.ArtifactSyncEvent
import com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier
import com.netflix.spinnaker.keel.api.plugins.supporting
import com.netflix.spinnaker.keel.config.ArtifactRefreshConfig
import com.netflix.spinnaker.keel.exceptions.InvalidSystemStateException
import com.netflix.spinnaker.keel.logging.TracingSupport.Companion.blankMDC
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.telemetry.recordDurationPercentile
import com.netflix.spinnaker.keel.telemetry.safeIncrement
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.supervisorScope
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

@EnableConfigurationProperties(ArtifactConfig::class, ArtifactRefreshConfig::class)
@Component
class ArtifactListener(
  private val repository: KeelRepository,
  private val artifactSuppliers: List<ArtifactSupplier<*, *>>,
  private val artifactConfig: ArtifactConfig,
  private val artifactRefreshConfig: ArtifactRefreshConfig,
  private val clock: Clock,
  private val spectator: Registry,
  private val workQueueProcessor: WorkQueueProcessor
) : CoroutineScope {

  override val coroutineContext: CoroutineContext = Dispatchers.IO

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  private val enabled = AtomicBoolean(false)

  @EventListener(ApplicationUp::class)
  fun onApplicationUp() {
    log.info("Application up, enabling scheduled artifact syncing")
    enabled.set(true)
  }

  @EventListener(ApplicationDown::class)
  fun onApplicationDown() {
    log.info("Application down, disabling scheduled artifact syncing")
    enabled.set(false)
  }

  /**
   * Fetch latest version of an artifact after it is registered.
   */
  @EventListener(ArtifactRegisteredEvent::class)
  fun onArtifactRegisteredEvent(event: ArtifactRegisteredEvent) {
    val artifact = event.artifact

    if (repository.artifactVersions(artifact, artifactConfig.defaultMaxConsideredVersions).isEmpty()) {
      val artifactSupplier = artifactSuppliers.supporting(artifact.type)

      val latestArtifact = runBlocking {
        log.debug("Retrieving latest version of registered artifact {}", artifact)
        artifactSupplier.getLatestArtifact(artifact.deliveryConfig, artifact)
      }

      if (latestArtifact != null) {
        log.debug("Storing latest version {} (status={}) for registered artifact {}", latestArtifact.version, latestArtifact.status, artifact)
        workQueueProcessor.enrichAndStore(latestArtifact, artifactSupplier)
      } else {
        log.warn("No artifact versions found for ${artifact.type}:${artifact.name}")
      }
    }
  }

  @EventListener(ArtifactSyncEvent::class)
  fun triggerArtifactSync(event: ArtifactSyncEvent) {
     if (event.controllerTriggered) {
      log.info("Fetching latest ${artifactRefreshConfig.limit} version(s) of all registered artifacts...")
    }
    syncLastLimitArtifactVersions()
  }

  /**
   * For each registered artifact, get the last [artifactRefreshConfig.limit] versions, and persist if it's newer than what we have.
   */
  @Scheduled(fixedDelayString = "\${keel.artifact-refresh.frequency:PT6H}")
  fun syncLastLimitArtifactVersions() {
    if (enabled.get()) {
      val startTime = clock.instant()
      val job = launch(blankMDC) {
        supervisorScope {
          runCatching {
            repository.artifactsDueForRefresh(
              artifactRefreshConfig.minAgeDuration,
              artifactRefreshConfig.batchSize
            )
          }
            .onFailure {
              log.error("Exception fetching artifacts due for refresh", it)
            }
            .onSuccess {
              it.forEach {
                launch {
                  try {
                    withTimeout(artifactRefreshConfig.timeoutDuration.toMillis()) {
                      refreshDeliveryArtifact(it)
                    }
                  } catch (e: TimeoutCancellationException) {
                    log.error("Timed out refreshing artifacts for ${it.deliveryConfigName}", e)
                    spectator.counter(
                      "keel.scheduled.timeout",
                      listOf(
                        BasicTag("type",  "artifactrefresh")
                      )
                    ).safeIncrement()
                  } catch (e: Exception) {
                    log.error("Error refreshing artifacts for ${it.deliveryConfigName}", e)
                    spectator.counter(
                      "keel.scheduled.failure",
                      listOf(
                        BasicTag("type",  "artifactrefresh")
                      )
                    ).safeIncrement()
                  }
                }
              }
              spectator.counter(
                "keel.scheduled.batch.size",
                listOf(BasicTag("type", "artifactrefresh"))
              ).increment(it.size.toLong())
            }
        }
      }

      runBlocking { job.join() }
      recordDuration(startTime, "artifactrefresh")
    }
  }

  private fun refreshDeliveryArtifact(artifact: DeliveryArtifact) {
    val lastStoredVersions = repository.artifactVersions(artifact, artifactRefreshConfig.limit)
    val currentVersions = lastStoredVersions.map { it.version }
    log.debug("Last recorded versions of $artifact: $currentVersions")

    val artifactSupplier = artifactSuppliers.supporting(artifact.type)
    val latestAvailableVersions = artifactSupplier.getLatestArtifacts(artifact.deliveryConfig, artifact, artifactRefreshConfig.limit)
    log.debug("Latest available versions of $artifact: ${latestAvailableVersions.map { it.version }}")

    val newVersions = latestAvailableVersions
      .filterNot { currentVersions.contains(it.normalized().version) }
      .filter { artifactSupplier.shouldProcessArtifact(it) }
    if (newVersions.isNotEmpty()) {
      log.debug("$artifact has a missing version(s) ${newVersions.map { it.version }}, persisting.")
      newVersions.forEach { workQueueProcessor.enrichAndStore(it, artifactSupplier) }
    } else {
      log.debug("No new versions to persist for $artifact")
    }
  }

  private val DeliveryArtifact.deliveryConfig: DeliveryConfig
    get() = this.deliveryConfigName
      ?.let { repository.getDeliveryConfig(it) }
      ?: throw InvalidSystemStateException("Delivery config name missing in artifact object")

  private fun recordDuration(startTime : Instant, type: String) =
    spectator.recordDurationPercentile("keel.scheduled.method.duration", clock, startTime, setOf(BasicTag("type", type)))
}
