package com.netflix.spinnaker.keel.artifacts

import brave.Tracer
import com.netflix.spinnaker.keel.api.events.ArtifactVersionDeploying
import com.netflix.spinnaker.keel.api.events.ArtifactVersionMarkedDeploying
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.logging.withCoroutineTracingContext
import com.netflix.spinnaker.keel.persistence.KeelRepository
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.time.Clock

@Component
class ArtifactDeployingListener(
  private val repository: KeelRepository,
  private val publisher: EventPublisher,
  private val clock: Clock,
  private val tracer: Tracer? = null
) {

  private val log = LoggerFactory.getLogger(javaClass)

  @EventListener(ArtifactVersionDeploying::class)
  fun onArtifactVersionDeploying(event: ArtifactVersionDeploying) =
    runBlocking {
      val resourceId = event.resourceId
      val resource = repository.getResource(resourceId)
      val deliveryConfig = repository.deliveryConfigFor(resourceId)
      val env = repository.environmentFor(resourceId)

      val artifact = resource.findAssociatedArtifact(deliveryConfig) ?: run {
        log.warn(
          "Resource $resource has no associated artifact, so we can't mark ${event.artifactVersion} as deploying."
        )
        return@runBlocking
      }

      withCoroutineTracingContext(artifact, event.artifactVersion, tracer) {
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
          publisher.publishEvent(
            ArtifactVersionMarkedDeploying(deliveryConfig, artifact, event.artifactVersion, env, clock.instant())
          )
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
}
