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
  private val workQueueProcessor: WorkQueueProcessor,
  private val tracer: Tracer? = null
): DiscoveryActivated() {

  /**
   * Fetch latest version of an artifact after it is registered.
   */
  @EventListener(ArtifactRegisteredEvent::class)
  fun onArtifactRegisteredEvent(event: ArtifactRegisteredEvent) {
    val artifact = event.artifact
    val artifactSupplier = artifactSuppliers.supporting(artifact.type)

    val latestVersions = runBlocking {
      log.debug("Retrieving latest versions of registered artifact {}", artifact)
      artifactSupplier.getLatestArtifacts(artifact.deliveryConfig, artifact, artifactRefreshConfig.firstLoadLimit)
    }

    if (latestVersions.isNotEmpty()) {
      // We are storing multiple versions in case that one of the environments is using an older version than latest
      log.debug("Storing latest {} versions of artifact {}", latestVersions.size, artifact)
      latestVersions.forEach {
        log.debug("Storing version {} (status={}) for registered artifact {}", it.version, it.status, artifact)
        workQueueProcessor.enrichAndStore(it, artifactSupplier)
      }
    } else {
      log.warn("No artifact versions found for ${artifact.type}:${artifact.name}")
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
        log.debug("Syncing last ${artifactRefreshConfig.scheduledSyncLimit} artifact version(s)...")
        repository.getAllArtifacts().forEach { artifact ->
          launch {
            syncLastLimitArtifactVersions(artifact, artifactRefreshConfig.scheduledSyncLimit)
          }
        }
      }
    }
  }

  suspend fun syncLastLimitArtifactVersions(artifact: DeliveryArtifact, limit: Int) {
    val lastStoredVersions = repository.artifactVersions(artifact, limit)
    val currentVersions = lastStoredVersions.map { it.version }
    log.debug("Last recorded versions of $artifact: $currentVersions")

    val artifactSupplier = artifactSuppliers.supporting(artifact.type)
    val latestAvailableVersions = artifactSupplier.getLatestArtifacts(artifact.deliveryConfig, artifact, limit)
    log.debug("Latest available versions of $artifact: ${latestAvailableVersions.map { it.version }}")

    val newVersions = latestAvailableVersions
      .filterNot { currentVersions.contains(it.version) }
      .filter { artifactSupplier.shouldProcessArtifact(it) }

    if (newVersions.isNotEmpty()) {
      newVersions.forEach { publishedArtifact ->
        withCoroutineTracingContext(publishedArtifact, tracer) {
          log.debug("Detected missing version ${publishedArtifact.version} of $artifact. Persisting.")
          workQueueProcessor.enrichAndStore(publishedArtifact, artifactSupplier)
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
