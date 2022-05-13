package com.netflix.spinnaker.keel.actuation

import brave.Tracer
import com.netflix.spinnaker.config.ArtifactConfig
import com.netflix.spinnaker.keel.actuation.EnvironmentConstraintRunner.EnvironmentContext
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PASS
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVetoes
import com.netflix.spinnaker.keel.core.api.PinnedEnvironment
import com.netflix.spinnaker.keel.core.api.PromotionStatus.CURRENT
import com.netflix.spinnaker.keel.core.api.PromotionStatus.DEPLOYING
import com.netflix.spinnaker.keel.logging.withCoroutineTracingContext
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.NoDeliveryConfigForApplication
import com.netflix.spinnaker.keel.telemetry.ArtifactVersionApproved
import com.netflix.spinnaker.keel.telemetry.EnvironmentCheckComplete
import com.newrelic.api.agent.Trace
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.cloud.sleuth.annotation.NewSpan
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration

/**
 * This class is responsible for approving artifacts in environments
 */
@Component
@EnableConfigurationProperties(ArtifactConfig::class)
class EnvironmentPromotionChecker(
  private val repository: KeelRepository,
  private val constraintRunner: EnvironmentConstraintRunner,
  private val publisher: ApplicationEventPublisher,
  private val artifactConfig: ArtifactConfig,
  private val clock: Clock,
  private val tracer: Tracer? = null
) {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  // todo eb: remove this bulk function once we've switch completely to temporal checking
  suspend fun checkEnvironments(deliveryConfig: DeliveryConfig) {
    val startTime = clock.instant()
    try {
      val pinnedEnvs: Map<String, PinnedEnvironment> = repository
        .pinnedEnvironments(deliveryConfig)
        .associateBy { envPinKey(it.targetEnvironment, it.artifact) }

      val vetoedArtifacts: Map<String, EnvironmentArtifactVetoes> = repository
        .vetoedEnvironmentVersions(deliveryConfig)
        .associateBy { envPinKey(it.targetEnvironment, it.artifact) }

      deliveryConfig
        .artifacts
        .associateWith { repository.artifactVersions(it, artifactConfig.defaultMaxConsideredVersions) }
        .forEach { (artifact, versions) ->
          if (versions.isEmpty()) {
            log.warn("No versions for ${artifact.type} artifact name ${artifact.name} and reference ${artifact.reference} are known")
          } else {
            deliveryConfig.environments.forEach { environment ->
              checkEnvironment(deliveryConfig, environment, artifact, versions, vetoedArtifacts, pinnedEnvs)
            }
          }
        }
    } finally {
      publisher.publishEvent(
        EnvironmentCheckComplete(
          application = deliveryConfig.application,
          duration = Duration.between(startTime, clock.instant())
        )
      )
    }
  }

  /**
   * Check all the artifacts used in a single environment in a delivery config.
   *
   * This is used for the temporal path, where we're checking environments individually
   * instead of checking every environment in a delivery config at once.
   */
  @NewSpan
  suspend fun checkEnvironment(application: String, environmentName: String) {
    val startTime = clock.instant()
    try {
      val deliveryConfig = repository.getDeliveryConfigForApplication(application)
      val pinnedEnvs: Map<String, PinnedEnvironment> = repository
        .pinnedEnvironments(deliveryConfig)
        .associateBy { envPinKey(it.targetEnvironment, it.artifact) }

      val vetoedArtifacts: Map<String, EnvironmentArtifactVetoes> = repository
        .vetoedEnvironmentVersions(deliveryConfig)
        .associateBy { envPinKey(it.targetEnvironment, it.artifact) }

      deliveryConfig
        .artifacts
        .associateWith { repository.artifactVersions(it, artifactConfig.defaultMaxConsideredVersions) }
        .forEach { (artifact, versions) ->
          if (versions.isEmpty()) {
            log.warn("No versions for ${artifact.type} artifact name ${artifact.name} and reference ${artifact.reference} are known")
          } else {
            deliveryConfig
              .environments
              .firstOrNull {it.name == environmentName}
              ?.also { environment ->
                checkEnvironment(deliveryConfig, environment, artifact, versions, vetoedArtifacts, pinnedEnvs)
              }

          }
        }
    } catch (e: NoDeliveryConfigForApplication) {
      log.error("Trying to check environment $environmentName of application $application, but there is no delivery config for this application. Is this an orphaned temporal workflow?")
    }
  }

  @Trace(dispatcher = true)
  private suspend fun checkEnvironment(
    deliveryConfig: DeliveryConfig,
    environment: Environment,
    artifact: DeliveryArtifact,
    versions: List<PublishedArtifact>,
    vetoedArtifacts: Map<String, EnvironmentArtifactVetoes>,
    pinnedEnvs: Map<String, PinnedEnvironment>
  ) {
    log.debug("Checking app ${deliveryConfig.application} env ${environment.name}")
    if (artifact.isUsedIn(environment)) {

      val latestVersions = versions.map { it.version }
      // note: we can't evaluate skipped versions here (yet) because we aren't reliably sorting artifacts by
      //   created time, so sometimes it seems we sort by approval time, which means that older versions roll
      //   out instead of newer versions :(
      val versionsToUse = repository
        .getPendingVersionsInEnvironment(
          deliveryConfig,
          artifact.reference,
          environment.name
        )
        .sortedWith(artifact.sortingStrategy.comparator)
        .map { it.version }
        .intersect(latestVersions) // only take newest ones so we avoid checking really old versions
        .toList()

      val envContext = EnvironmentContext(
        deliveryConfig = deliveryConfig,
        environment = environment,
        artifact = artifact,
        versions = versionsToUse,
        vetoedVersions = (vetoedArtifacts[envPinKey(environment.name, artifact)]?.versions?.map { it.version })?.toSet()
          ?: emptySet()
      )

      val pinnedVersion = pinnedEnvs.versionFor(environment.name, artifact)

      if (pinnedVersion != null) {
        withCoroutineTracingContext(artifact, pinnedVersion, tracer) {
          // approve version first to fast track deployment
          approveVersion(deliveryConfig, artifact, pinnedVersion, environment)
          triggerResourceRecheckForPinnedVersion(deliveryConfig, artifact, pinnedVersion, environment)
        }
      }

      constraintRunner.checkEnvironment(envContext)

      // everything the constraint runner has already approved
      val queuedForApproval = repository
        .getArtifactVersionsQueuedForApproval(deliveryConfig.name, environment.name, artifact)
        .run {
          if (pinnedVersion != null) {
            filter { artifactVersion ->
              artifactVersion.version != pinnedVersion // no need to approve the pinned version
            }
          } else {
            this
          }
        }
        .toMutableList()

      /**
       * Approve all constraints starting with oldest first so that the ordering is
       * maintained.
       */
      queuedForApproval
        .reversed()
        .forEach { artifactVersion ->
          withCoroutineTracingContext(artifactVersion, tracer) {
            /**
             * We don't need to re-invoke stateful constraint evaluators for these, but we still
             * check stateless constraints to avoid approval outside of allowed-times.
             */
            log.debug(
              "Version ${artifactVersion.version} of artifact ${artifact.name} is queued for approval, " +
                "and being evaluated for stateless constraints in environment ${environment.name}"
            )

            val passesStatelessConstraints = constraintRunner.checkStatelessConstraints(
              artifact, deliveryConfig, artifactVersion.version, environment
            )

            if (passesStatelessConstraints) {
              approveVersion(deliveryConfig, artifact, artifactVersion.version, environment)
              repository.deleteArtifactVersionQueuedForApproval(
                deliveryConfig.name, environment.name, artifact, artifactVersion.version
              )
            } else {
              log.debug(
                "Version ${artifactVersion.version} of $artifact does not currently pass stateless constraints in environment ${environment.name}"
              )
              queuedForApproval.remove(artifactVersion)
            }
          }
        }

      if (pinnedVersion == null) {
        val versionSelected = queuedForApproval.firstOrNull()
        if (versionSelected == null) {
          log.warn("No version of {} passes constraints for environment {}", artifact, environment.name)
        }
        triggerResourceRecheckForVetoedVersion(
          deliveryConfig,
          artifact,
          environment,
          vetoedArtifacts[envPinKey(environment.name, artifact)]
        )
      }
    } else {
      log.debug("Skipping checks for {} as it is not used in environment {}", artifact, environment.name)
    }
  }

  private fun triggerResourceRecheckForVetoedVersion(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    targetEnvironment: Environment,
    vetoedArtifacts: EnvironmentArtifactVetoes?
  ) {
    if (vetoedArtifacts == null) return
    val currentVersion = repository.getCurrentlyDeployedArtifactVersion(deliveryConfig, artifact, targetEnvironment.name)?.version
    if (vetoedArtifacts.versions.map { it.version }.contains(currentVersion)) {
      log.info("Triggering recheck for environment ${targetEnvironment.name} of application ${deliveryConfig.application} that is currently on a vetoed version of ${artifact.reference}")
      // trigger a recheck of the resources if the current version is vetoed
      repository.triggerResourceRecheck(targetEnvironment.name, deliveryConfig)
    }
  }

  private fun triggerResourceRecheckForPinnedVersion(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: Environment
  ) {
    val status = repository.getArtifactPromotionStatus(deliveryConfig, artifact, version, targetEnvironment.name)
    if (status !in listOf(CURRENT, DEPLOYING)) {
      log.info("Triggering recheck for pinned environment ${targetEnvironment.name} of application ${deliveryConfig.application} that are on the wrong version. Pinned version $version of ${artifact.reference}")
      // trigger a recheck of the resources if the version isn't already on its way to the environment
      repository.triggerResourceRecheck(targetEnvironment.name, deliveryConfig)
    }
  }

  private suspend fun approveVersion(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: Environment
  ) {
    log.debug("Approving application ${deliveryConfig.application} version $version of ${artifact.reference} in environment ${targetEnvironment.name}")
    val isNewVersion = repository
      .approveVersionFor(deliveryConfig, artifact, version, targetEnvironment.name)
    if (isNewVersion) {
      log.info(
        "Approved {} {} version {} for {} environment {} in {}",
        artifact.name,
        artifact.type,
        version,
        deliveryConfig.name,
        targetEnvironment.name,
        deliveryConfig.application
      )

      publisher.publishEvent(
        ArtifactVersionApproved(
          deliveryConfig.application,
          deliveryConfig.name,
          targetEnvironment.name,
          artifact.name,
          artifact.type,
          version
        )
      )

      // persist the status of stateless constraints because their current value is all we care about
      snapshotStatelessConstraintStatus(deliveryConfig, artifact, version, targetEnvironment)

      // recheck all resources in an environment, so action can be taken right away
      repository.triggerResourceRecheck(targetEnvironment.name, deliveryConfig)
    }
  }

  /**
   * Save the passing status of all stateless constraints when a version is approved so that
   * their status stays the same forever. We don't want them to be evaluated anymore.
   */
  private suspend fun snapshotStatelessConstraintStatus(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    environment: Environment
  ) {
    constraintRunner.getStatelessConstraintSnapshots(
      artifact = artifact,
      deliveryConfig = deliveryConfig,
      version = version,
      environment = environment,
      currentStatus = PASS // We just checked that all these pass since a version was approved
    ).forEach { constraintState ->
      log.debug("Storing final constraint state snapshot for {} constraint for {} version {} for {} environment {} in {}",
        constraintState.type,
        artifact,
        version,
        deliveryConfig.name,
        environment.name,
        deliveryConfig.application
      )
      repository.storeConstraintState(constraintState)
    }

  }

  private fun Map<String, PinnedEnvironment>.hasPinFor(
    environmentName: String,
    artifact: DeliveryArtifact
  ): Boolean {
    if (isEmpty()) {
      return false
    }

    val key = envPinKey(environmentName, artifact)
    return containsKey(key) && checkNotNull(get(key)).artifact == artifact
  }

  private fun Map<String, PinnedEnvironment>.versionFor(
    environmentName: String,
    artifact: DeliveryArtifact
  ): String? =
    get(envPinKey(environmentName, artifact))?.version

  fun envPinKey(environmentName: String, artifact: DeliveryArtifact): String =
    "$environmentName:${artifact.reference}"
}
