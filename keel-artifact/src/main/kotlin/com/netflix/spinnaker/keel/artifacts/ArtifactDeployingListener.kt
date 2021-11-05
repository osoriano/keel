package com.netflix.spinnaker.keel.artifacts

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.api.events.ArtifactVersionDeploying
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.telemetry.recordDuration
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.time.Clock

@Component
class ArtifactDeployingListener(
  private val repository: KeelRepository,
  private val spectator: Registry,
  private val clock: Clock
) {

  private val log = LoggerFactory.getLogger(javaClass)

  companion object {
    internal const val ARTIFACT_DEPLOYMENT_DELAY = "artifact.deployment.delay"
  }

  @EventListener(ArtifactVersionDeploying::class)
  fun onArtifactVersionDeploying(event: ArtifactVersionDeploying) =
    runBlocking {
      val resourceId = event.resourceId
      val resource = repository.getResource(resourceId)
      val deliveryConfig = repository.deliveryConfigFor(resourceId)
      val env = repository.environmentFor(resourceId)

      // if there's no artifact associated with this resource, we do nothing.
      val artifact = resource.findAssociatedArtifact(deliveryConfig) ?: return@runBlocking

      val pinnedVersion = repository.getPinnedVersion(deliveryConfig, env.name, artifact.reference)
      val action = if (event.artifactVersion == pinnedVersion) "pinned" else "approved"

      val approvedAt = repository.getApprovedAt(
        deliveryConfig = deliveryConfig,
        artifact = artifact,
        version = event.artifactVersion,
        targetEnvironment = env.name
      )

      if (approvedAt != null) {
        log.info("Marking {} as deploying in {} for config {}", event.artifactVersion, env.name, deliveryConfig.name)
        repository.markAsDeployingTo(
          deliveryConfig = deliveryConfig,
          artifact = artifact,
          version = event.artifactVersion,
          targetEnvironment = env.name
        )
        // record how long it took us to deploy this version since it was approved
        spectator.recordDuration(ARTIFACT_DEPLOYMENT_DELAY, clock, approvedAt, setOf(BasicTag("action", action)))
      } else {
        log.warn(
          "Somehow {} is deploying in {} for config {} without being approved for that environment",
          event.artifactVersion,
          env.name,
          deliveryConfig.name
        )
      }
    }
}
