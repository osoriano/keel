package com.netflix.spinnaker.keel.artifacts

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.events.ArtifactVersionDeploying
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.telemetry.ARTIFACT_DELAY
import com.netflix.spinnaker.keel.telemetry.recordDuration
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration

@Component
class ArtifactDeployingListener(
  private val repository: KeelRepository,
  private val spectator: Registry,
  private val clock: Clock
) {

  private val log = LoggerFactory.getLogger(javaClass)

  @EventListener(ArtifactVersionDeploying::class)
  fun onArtifactVersionDeploying(event: ArtifactVersionDeploying) =
    runBlocking {
      val resourceId = event.resourceId
      val resource = repository.getResource(resourceId)
      val deliveryConfig = repository.deliveryConfigFor(resourceId)
      val env = repository.environmentFor(resourceId)

      // if there's no artifact associated with this resource, we do nothing.
      val artifact = resource.findAssociatedArtifact(deliveryConfig) ?: return@runBlocking

      val approvedForEnv = repository.isApprovedFor(
        deliveryConfig = deliveryConfig,
        artifact = artifact,
        version = event.artifactVersion,
        targetEnvironment = env.name
      )

      if (approvedForEnv) {
        log.info("Marking {} as deploying in {} for config {}", event.artifactVersion, env.name, deliveryConfig.name)
        repository.markAsDeployingTo(
          deliveryConfig = deliveryConfig,
          artifact = artifact,
          version = event.artifactVersion,
          targetEnvironment = env.name
        )
        // record how long it took us to deploy this version since it was approved
        recordDeploymentDelay(deliveryConfig, artifact, event.artifactVersion, env.name)
      } else {
        log.warn(
          "Somehow {} is deploying in {} for config {} without being approved for that environment",
          event.artifactVersion,
          env.name,
          deliveryConfig.name
        )
      }
    }

  private fun recordDeploymentDelay(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String
  ) {
    val approvedAt = repository.getApprovedAt(deliveryConfig, artifact, version, targetEnvironment)
    val pinnedAt = repository.getPinnedAt(deliveryConfig, artifact, version, targetEnvironment)
    val startTime = when {
      pinnedAt != null && approvedAt == null -> pinnedAt
      pinnedAt == null && approvedAt != null -> approvedAt
      pinnedAt != null && approvedAt != null -> when {
        pinnedAt.isAfter(approvedAt) -> pinnedAt
        else -> approvedAt
      }
      else -> null
    }

    if (startTime != null) {
      log.debug("Recording deployment delay for $artifact: ${Duration.between(startTime, clock.instant())}")
      spectator.recordDuration(ARTIFACT_DELAY, clock, startTime,
        "delayType" to "deployment",
        "artifactType" to artifact.type,
        "artifactName" to artifact.name,
        "action" to if (startTime == pinnedAt) "pinned" else "approved"
      )
    }
  }
}
