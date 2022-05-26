package com.netflix.spinnaker.keel.artifacts

import brave.Tracer
import com.netflix.spinnaker.config.ArtifactConfig
import com.netflix.spinnaker.keel.activation.DiscoveryActivated
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.events.ArtifactRegisteredEvent
import com.netflix.spinnaker.keel.api.events.AllArtifactsSyncEvent
import com.netflix.spinnaker.keel.api.events.ArtifactSyncEvent
import com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier
import com.netflix.spinnaker.keel.api.plugins.supporting
import com.netflix.spinnaker.keel.config.ArtifactRefreshConfig
import com.netflix.spinnaker.keel.exceptions.InvalidSystemStateException
import com.netflix.spinnaker.keel.logging.withCoroutineTracingContext
import com.netflix.spinnaker.keel.persistence.KeelRepository
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@EnableConfigurationProperties(ArtifactConfig::class, ArtifactRefreshConfig::class)
@Component
class ArtifactListener(
  private val repository: KeelRepository,
  private val artifactSuppliers: List<ArtifactSupplier<*, *>>,
  private val artifactRefreshConfig: ArtifactRefreshConfig,
  private val artifactQueueProcessor: ArtifactQueueProcessor,
  private val tracer: Tracer? = null
): DiscoveryActivated() {

  /**
   * Fetch and store the latest versions of an artifact after it is registered
   */
  @EventListener(ArtifactRegisteredEvent::class)
  fun onArtifactRegisteredEvent(event: ArtifactRegisteredEvent) {
    runBlocking {
      syncLastLimitArtifactVersions(event.artifact, artifactRefreshConfig.firstLoadLimit)
    }
  }

  @EventListener(AllArtifactsSyncEvent::class)
  fun triggerAllArtifactsSync(event: AllArtifactsSyncEvent) {
     if (event.controllerTriggered) {
      log.info("Fetching latest ${artifactRefreshConfig.scheduledSyncLimit} version(s) of all registered artifacts...")
    }
    syncLastLimitAllArtifactsVersions()
  }

  @EventListener(ArtifactSyncEvent::class)
  fun triggerArtifactSync(event: ArtifactSyncEvent) {
    log.info("Fetching latest ${event.limit} version(s) of ${event.artifactReference} in app ${event.application}...")
    val config = repository.getDeliveryConfigForApplication(event.application)
    val artifact = repository.getArtifact(config.name, event.artifactReference)
    runBlocking {
      syncLastLimitArtifactVersions(artifact, event.limit)
    }
  }

  /**
   * For each registered artifact, get the last [ArtifactRefreshConfig.scheduledSyncLimit] versions, and persist if it's newer than what we have.
   */
  @Scheduled(fixedDelayString = "\${keel.artifact-refresh.frequency:PT6H}")
  fun syncLastLimitAllArtifactsVersions() {
    if (enabled.get()) {
      runBlocking {
        log.debug("Syncing last ${artifactRefreshConfig.scheduledSyncLimit} artifact version(s) for all known artifacts...")
        repository.getAllArtifacts().forEach { artifact ->
          launch {
            syncLastLimitArtifactVersions(artifact, artifactRefreshConfig.scheduledSyncLimit)
          }
        }
      }
    }
  }

  suspend fun syncLastLimitArtifactVersions(artifact: DeliveryArtifact, limit: Int) {
    if (artifact.isDryRun) {
      log.debug("Not syncing versions for dry-run artifact $artifact")
      return
    }

    log.debug("Syncing last $limit versions of $artifact")
    val lastStoredVersions = repository.artifactVersions(artifact, limit).map { it.version }
    log.debug("Last recorded versions of $artifact: $lastStoredVersions")

    val artifactSupplier = artifactSuppliers.supporting(artifact.type)
    val latestAvailableVersions = artifactSupplier.getLatestVersions(artifact.deliveryConfig, artifact, limit)
    log.debug("Latest available versions of $artifact: ${latestAvailableVersions.map { it.version }}")

    val newVersions = latestAvailableVersions
      .filterNot { lastStoredVersions.contains(it.version) }
      .filter { artifactSupplier.shouldProcessArtifact(it) }

    if (newVersions.isNotEmpty()) {
      newVersions.forEach { publishedArtifact ->
        withCoroutineTracingContext(publishedArtifact, tracer) {
          log.debug("Detected missing version ${publishedArtifact.version} of $artifact. Persisting.")
          artifactQueueProcessor.enrichAndStore(publishedArtifact, artifactSupplier)
        }
      }
    } else {
      log.debug("No new versions to persist for $artifact")
    }
  }

  private val DeliveryArtifact.deliveryConfig: DeliveryConfig
    get() = this.deliveryConfigName
      ?.let { repository.getDeliveryConfig(it) }
      ?: throw InvalidSystemStateException("Delivery config name missing in artifact object")
}
