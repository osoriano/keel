package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.api.events.ArtifactVersionDeployed
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.core.api.PromotionStatus.CURRENT
import com.netflix.spinnaker.keel.events.ArtifactDeployedNotification
import com.netflix.spinnaker.keel.persistence.KeelRepository
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.time.Instant

/**
 * Listens to [ArtifactVersionDeployed] events to update the status of the artifact in the database and
 * relay the event as an [ArtifactDeployedNotification] that can then be sent to end users.
 */
@Component
class ArtifactDeployedListener(
  private val repository: KeelRepository,
  val publisher: EventPublisher
) {
  private val log = LoggerFactory.getLogger(javaClass)

  private fun approveAllConstraintsForVersion(
    deliveryConfigName: String,
    environmentName: String,
    artifactVersion: String,
    artifactReference: String
  ) {
    repository.constraintStateFor(
      deliveryConfigName = deliveryConfigName,
      environmentName = environmentName,
      artifactReference = artifactReference,
      artifactVersion = artifactVersion
    ).forEach {
      if (!it.status.complete) {
        log.debug("Updating app $deliveryConfigName constraint ${it.type} to OVERRIDE_PASS")
        repository.storeConstraintState(
          it.copy(
            status = ConstraintStatus.OVERRIDE_PASS,
            judgedBy = "keel@spinnaker.io",
            judgedAt = Instant.now()
          )
        )
      }
    }
  }

  @EventListener(ArtifactVersionDeployed::class)
  fun onArtifactVersionDeployed(event: ArtifactVersionDeployed) =
    runBlocking {
      val resourceId = event.resourceId
      val resource = repository.getResource(resourceId)
      val deliveryConfig = repository.deliveryConfigFor(resourceId)
      val env = repository.environmentFor(resourceId)

      val artifact = resource.findAssociatedArtifact(deliveryConfig)

      // if there's no artifact associated with this resource, we do nothing.
      if (artifact == null) {
        log.debug("Unable to find artifact associated with resource $resourceId in application ${deliveryConfig.application}")
        return@runBlocking
      }

      val approvedForEnv = repository.isApprovedFor(
        deliveryConfig = deliveryConfig,
        artifact = artifact,
        version = event.artifactVersion,
        targetEnvironment = env.name
      )

      fun hasNoApprovedVersions() = repository.getLatestApprovedInEnvArtifactVersion(
        config = deliveryConfig,
        artifact = artifact,
        environmentName = env.name,
        excludeCurrent = false
      ) == null

      // We add a special case for migrated apps that already have a version deployed but nothing is approved in the DB.
      // In this case, we still want to mark the version as deployed
      if (approvedForEnv || hasNoApprovedVersions()) {
        if (!approvedForEnv) {
          log.info(
            "This is the first and already deployed of artifact {} - {} in env {} of app {}",
            artifact.reference,
            event.artifactVersion,
            env.name,
            deliveryConfig.name
          )
          approveAllConstraintsForVersion(
            deliveryConfigName = deliveryConfig.name,
            environmentName = env.name,
            artifactReference = artifact.reference,
            artifactVersion = event.artifactVersion
          )
        }
        val markedCurrentlyDeployed = repository.getArtifactPromotionStatus(
          deliveryConfig = deliveryConfig,
          artifact = artifact,
          version = event.artifactVersion,
          targetEnvironment = env.name
        ) == CURRENT
        if (!markedCurrentlyDeployed) {
          log.info("Marking {} as deployed in {} for config {} because it's not currently marked as deployed", event.artifactVersion, env.name, deliveryConfig.name)
          repository.markAsSuccessfullyDeployedTo(
            deliveryConfig = deliveryConfig,
            artifact = artifact,
            version = event.artifactVersion,
            targetEnvironment = env.name
          )
          publisher.publishEvent(
            ArtifactDeployedNotification(
              config = deliveryConfig,
              deliveryArtifact = artifact,
              artifactVersion = event.artifactVersion,
              targetEnvironment = env
            )
          )
        } else {
          log.debug("$artifact version ${event.artifactVersion} is already marked as deployed to $env in" +
            " application ${deliveryConfig.application}")
        }
      } else {
        log.debug("$artifact version ${event.artifactVersion} is not approved for $env in application" +
          " ${deliveryConfig.application}, so not marking as deployed.")
      }
    }
}
