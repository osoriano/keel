package com.netflix.spinnaker.keel.resolvers

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.constraints.DeploymentConstraintEvaluator
import com.netflix.spinnaker.keel.api.plugins.ConstraintEvaluator
import com.netflix.spinnaker.keel.core.ResourceCurrentlyUnresolvable
import com.netflix.spinnaker.keel.persistence.KeelRepository
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Helper to consistently resolve the desired version of an artifact in an environement.
 * Used by the titus and ec2 image resolvers.
 */
@Component
class DesiredVersionResolver(
  open val repository: KeelRepository,
  open val featureToggles: FeatureToggles,
  open val deploymentConstraintEvaluators: List<DeploymentConstraintEvaluator<*, *>>
) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  /**
   * This is where we choose what version should be in the desired state.
   * This takes into account approved versions.
   */
  fun getDesiredVersion(
    deliveryConfig: DeliveryConfig,
    environment: Environment,
    artifact: DeliveryArtifact
  ): String {
    if (featureToggles.isEnabled(FeatureToggles.DEPLOYMENT_CONSTRAINTS, false)) {
      val identifier = "${artifact.reference} in env ${environment.name} in application ${deliveryConfig.application}"
      log.debug("Evaluating deployment constraints for {}", identifier)
      val pinnedVersion = repository.getPinnedVersion(deliveryConfig, environment.name, artifact.reference)
      if (pinnedVersion != null) {
        log.debug("Deploying pinned version $pinnedVersion for {}", identifier)
        // pinned version overrides deployment constraints.
        // this logic is taken care of in `latestVersionApprovedIn` for approved versions
        return pinnedVersion
      }

      // first we check all versions that are approved and newer than the latest deploying or current version
      val candidates = repository.deploymentCandidateVersions(deliveryConfig, artifact, environment.name)

      val candidateToDeploy = candidates.firstOrNull { version ->
        val passesDeploymentConstraints = runBlocking {
          checkConstraintWhenSpecified(artifact, deliveryConfig, version, environment)
        }
        passesDeploymentConstraints // take the first one that passes
      }
      if (candidateToDeploy != null) {
        val olderVersions = candidates.takeLastWhile { it != candidateToDeploy }
        markOlderVersionsAsSkipped(olderVersions, artifact, deliveryConfig, environment)
      }

      // if none of the candidate versions can be approved, select the latest version that started deploying
      // in the environment, because that will be the desired version.
      val versionToDeploy = candidateToDeploy
        ?: repository.latestDeployableVersionIn(deliveryConfig, artifact, environment.name)

      return versionToDeploy ?: throw NoDeployableVersionForEnvironment(artifact, environment.name, candidates)

    } else {
      // current behavior, unchanged.
      // todo eb: remove once deployment constraints has been vetted.
      return repository.latestVersionApprovedIn(
        deliveryConfig,
        artifact,
        environment.name
      ) ?: throw NoApprovedVersionForEnvironment(artifact, environment.name)
    }
  }

  fun markOlderVersionsAsSkipped(
    olderVersions: List<String>,
    artifact: DeliveryArtifact,
    deliveryConfig: DeliveryConfig,
    environment: Environment
  ) {
    deploymentConstraintEvaluators.forEach { evaluator ->
      if (environment.hasSupportedConstraint(evaluator)) {
        log.debug("Marking versions $olderVersions as skipped using ${evaluator.javaClass.simpleName} for application ${deliveryConfig.application}, " +
          "environment ${environment.name} artifact ${artifact.reference}")
        olderVersions.forEach { version ->
          evaluator.markVersionAsSkipped(artifact, version, deliveryConfig, environment)
        }
      }
    }
  }

  /**
   * Checks constraints for a list of evaluators.
   * Evaluates the constraint only if it's defined on the environment.
   * @return true if all constraints pass
   */
  suspend fun checkConstraintWhenSpecified(
    artifact: DeliveryArtifact,
    deliveryConfig: DeliveryConfig,
    version: String,
    environment: Environment,
  ): Boolean {
    var allPass = true
    // we want to run all evaluators even if some fail so that the user has up to date info about the status
    deploymentConstraintEvaluators.forEach { evaluator ->
      val canPromote = !environment.hasSupportedConstraint(evaluator) ||
        evaluator.canDeploy(artifact, version, deliveryConfig, environment)

      if (environment.hasSupportedConstraint(evaluator)) {
        log.debug("Evaluating constraint (deployment)(user-specified) using ${evaluator.javaClass.simpleName} for application ${deliveryConfig.application}, " +
          "environment ${environment.name} artifact ${artifact.reference} version $version: canPromote=$canPromote")
      }

      if (!canPromote) {
        allPass = false
      }
    }
    return allPass
  }

  private fun Environment.hasSupportedConstraint(constraintEvaluator: ConstraintEvaluator<*>) =
    constraints.any { it.javaClass.isAssignableFrom(constraintEvaluator.supportedType.type) }
}

class NoApprovedVersionForEnvironment(
  val artifact: DeliveryArtifact,
  val environment: String
) : ResourceCurrentlyUnresolvable("No version found for artifact ${artifact.name} (ref: ${artifact.reference}) that satisfies constraints in environment $environment. Are there any approved versions?")

class NoDeployableVersionForEnvironment(
  val artifact: DeliveryArtifact,
  val environment: String,
  val approvedVersions: List<String>
) : ResourceCurrentlyUnresolvable("No version found for artifact  ${artifact.name} (ref: ${artifact.reference}) that satisfies deployment constraints in environment $environment. Approved versions: $approvedVersions")
