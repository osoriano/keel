package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.config.ArtifactConfig
import com.netflix.spinnaker.keel.activation.DiscoveryActivated
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.events.ArtifactRegisteredEvent
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
  private val artifactConfig: ArtifactConfig,
  private val artifactRefreshConfig: ArtifactRefreshConfig,
  private val workQueueProcessor: WorkQueueProcessor
): DiscoveryActivated() {

  /**
   * Fetch latest version of an artifact after it is registered.
   */
  @EventListener(ArtifactRegisteredEvent::class)
  fun onArtifactRegisteredEvent(event: ArtifactRegisteredEvent) {
    val artifact = event.artifact
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

  @EventListener(ArtifactSyncEvent::class)
  fun triggerArtifactSync(event: ArtifactSyncEvent) {
     if (event.controllerTriggered) {
      log.info("Fetching latest ${artifactRefreshConfig.limit} version(s) of all registered artifacts...")
    }
    syncLastLimitArtifactVersions()
  }

  /**
   * For each registered artifact, get the last [ArtifactRefreshConfig.limit] versions, and persist if it's newer than what we have.
   */
  @Scheduled(fixedDelayString = "\${keel.artifact-refresh.frequency:PT6H}")
  fun syncLastLimitArtifactVersions() {
    if (enabled.get()) {
      runBlocking {
        log.debug("Syncing last ${artifactRefreshConfig.limit} artifact version(s)...")
        repository.getAllArtifacts().forEach { artifact ->
          launch {
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
              newVersions.forEach { publishedArtifact ->
                withCoroutineTracingContext(publishedArtifact) {
                  log.debug("Detected missing version ${publishedArtifact.version} of $artifact. Persisting.")
                  workQueueProcessor.enrichAndStore(publishedArtifact, artifactSupplier)
                }
              }
            } else {
              log.debug("No new versions to persist for $artifact")
            }
          }
        }
      }
    }
  }

  private val DeliveryArtifact.deliveryConfig: DeliveryConfig
    get() = this.deliveryConfigName
      ?.let { repository.getDeliveryConfig(it) }
      ?: throw InvalidSystemStateException("Delivery config name missing in artifact object")
}
