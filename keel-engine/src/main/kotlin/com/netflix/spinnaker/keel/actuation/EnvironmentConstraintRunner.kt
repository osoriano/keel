package com.netflix.spinnaker.keel.actuation

import brave.Tracer
import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.anyStateful
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.constraints.ConstraintState
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.api.constraints.StatelessConstraintEvaluator
import com.netflix.spinnaker.keel.api.plugins.ApprovalConstraintEvaluator
import com.netflix.spinnaker.keel.api.plugins.ConstraintEvaluator
import com.netflix.spinnaker.keel.constraints.ConstraintEvaluators
import com.netflix.spinnaker.keel.logging.withCoroutineTracingContext
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.newrelic.api.agent.Trace
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

/**
 * This class is responsible for running constraints and queueing them for approval.
 * Approval into an environment is the responsibility of the [EnvironmentPromotionChecker].
 */
@Component
class EnvironmentConstraintRunner(
  private val repository: KeelRepository,
  private val constraintEvaluators: ConstraintEvaluators,
  private val tracer: Tracer? = null
) {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  private lateinit var implicitConstraints: List<ApprovalConstraintEvaluator<*>>
  private lateinit var explicitConstraints: List<ApprovalConstraintEvaluator<*>>

  // constraints that are only run if they are defined in a delivery config
  private lateinit var statefulEvaluators: List<ApprovalConstraintEvaluator<*>>
  private lateinit var statelessEvaluators: List<ApprovalConstraintEvaluator<*>>

  // constraints that run for every environment in a delivery config but aren't shown to the user.
  private lateinit var implicitStatefulEvaluators: List<ApprovalConstraintEvaluator<*>>
  private lateinit var implicitStatelessEvaluators: List<ApprovalConstraintEvaluator<*>>

  @EventListener(ApplicationReadyEvent::class)
  fun onReady() {
    implicitConstraints = constraintEvaluators.approvalEvaluators()
      .filter { it.isImplicit() }
    explicitConstraints = constraintEvaluators.approvalEvaluators()
      .filter { !it.isImplicit() }
    statefulEvaluators = explicitConstraints
      .filter { it.isStateful() }
    statelessEvaluators = explicitConstraints
      .filterNot { it.isStateful() }
    implicitStatefulEvaluators = implicitConstraints
      .filter { it.isStateful() }
    implicitStatelessEvaluators = implicitConstraints
      .filterNot { it.isStateful() }
  }

  /**
   * Checks the environment and determines the version that should be approved,
   * or null if there is no version that passes the constraints for an env + artifact combo.
   * Queues that version for approval if it exists.
   */
  @Trace(async=true)
  suspend fun checkEnvironment(
    envContext: EnvironmentContext
  ) {
    log.debug("Checking constraints of app ${envContext.deliveryConfig.name} env ${envContext.environment.name} for artifact ${envContext.artifact.name} versions ${envContext.versions}")
    val versionsWithPendingStatefulConstraintStatus: MutableList<PublishedArtifact> =
      if (envContext.environment.constraints.anyStateful) {
        repository
          .getPendingConstraintsForArtifactVersions(envContext.deliveryConfig.name, envContext.environment.name, envContext.artifact)
          .filter { envContext.versions.contains(it.version) }
          .toMutableList()
      } else {
        mutableListOf()
      }

    checkConstraints(
      envContext,
      versionsWithPendingStatefulConstraintStatus
    )

    /*
     * If there are pending constraints for prior versions, that we didn't recheck in the process of
     * finding the latest version above, recheck in ascending version order
     * so they can be timed out, failed, or approved.
     */
    handleOlderPendingVersions(envContext, versionsWithPendingStatefulConstraintStatus)
  }

  /**
   * Looks at all versions for an environment, and determines the latest version that passes constraints,
   * or null if none pass.
   *
   * In the process we check and evaluate constraints for each version.
   *
   * If a version passes all constraints it is queued for approval.
   */
  private suspend fun checkConstraints(
    envContext: EnvironmentContext,
    versionsWithPendingStatefulConstraintStatus: MutableList<PublishedArtifact>
  ) {
    val selectedVersion: String?
    var versionIsPending = false
    val vetoedVersions: Set<String> = envContext.vetoedVersions

    log.debug(
      "Checking constraints for ${envContext.artifact} in ${envContext.environment.name}, " +
        "versions=${envContext.versions.joinToString()}, vetoed=${envContext.vetoedVersions.joinToString()}"
    )

    selectedVersion = envContext.versions // all versions
      .filterNot { vetoedVersions.contains(it) }
      .firstOrNull { version ->
        withCoroutineTracingContext(envContext.artifact, version, tracer) {
          versionsWithPendingStatefulConstraintStatus.removeIf { it.version == version } // remove to indicate we are rechecking this version

          val passesStatelessConstraints = checkStatelessConstraints(envContext.artifact, envContext.deliveryConfig, version, envContext.environment)
          val passesStatefulConstraints = if (passesStatelessConstraints) {
            /**
             * Important! Only check stateful evaluators if all stateless evaluators pass. We don't
             * want to request judgement or deploy a canary for artifacts that aren't
             * deployed to a required environment.
             */
            checkStatefulConstraints(envContext.artifact, envContext.deliveryConfig, version, envContext.environment)
          } else {
            false
          }

          val passesConstraints = passesStatelessConstraints && passesStatefulConstraints

          if (envContext.environment.constraints.anyStateful) {
            versionIsPending = repository
              .constraintStateFor(envContext.deliveryConfig.name, envContext.environment.name, version, envContext.artifact.reference)
              .any { it.status == ConstraintStatus.PENDING }
          }

          logConstraintPassFail(
            passesStateless = passesStatelessConstraints,
            passesStateful = passesStatefulConstraints,
            version = version,
            envContext = envContext
          )

          log.debug(
            "Version $version of ${envContext.artifact} ${if (versionIsPending) "is" else "is not"} " +
              "pending constraint approval in ${envContext.environment.name}"
          )

          // select either the first version that passes all constraints,
          // or the first version where stateful constraints are pending.
          // so that we don't roll back while constraints are evaluating for a version
          passesConstraints || versionIsPending
        }
      }

    if (selectedVersion != null && !versionIsPending) {
      // we've selected a version that passes all constraints, queue it for approval
      withCoroutineTracingContext(envContext.artifact, selectedVersion, tracer) {
        queueForApproval(envContext.deliveryConfig, envContext.artifact, selectedVersion, envContext.environment.name)
      }
    }
  }

  fun logConstraintPassFail(passesStateless: Boolean, passesStateful: Boolean, version: String, envContext: EnvironmentContext) {
    val passesConstraints = passesStateful && passesStateless
    log.debug(
      "Version $version of ${envContext.artifact} ${passesConstraints.passFailWording()} " +
        "constraints in ${envContext.environment.name}"
    )
    if (!passesConstraints) {
      log.debug("Version $version of ${envContext.artifact} ${passesStateless.passFailWording()} stateless constraints " +
        "and ${passesStateful.passFailWording()} stateful constraints in environment ${envContext.environment.name}")
    }
  }

  fun Boolean.passFailWording() =
    if (this) "passes" else "does not pass"

  /**
   * Re-checks older versions with pending stateful constraints to see if they can be approved,
   * queues them for approval if they pass
   */
  private suspend fun handleOlderPendingVersions(
    envContext: EnvironmentContext,
    versionsWithPendingStatefulConstraintStatus: MutableList<PublishedArtifact>
  ) {
    log.debug("pendingVersionsToCheck: [${versionsWithPendingStatefulConstraintStatus.map { it.version }}] of artifact ${envContext.artifact.name} for environment ${envContext.environment.name} ")
    versionsWithPendingStatefulConstraintStatus
      .reversed() // oldest first
      .forEach { artifactVersion ->
        withCoroutineTracingContext(artifactVersion, tracer) {
          val passesConstraints =
            checkStatelessConstraints(envContext.artifact, envContext.deliveryConfig, artifactVersion.version, envContext.environment) &&
              checkStatefulConstraints(envContext.artifact, envContext.deliveryConfig, artifactVersion.version, envContext.environment)

          if (passesConstraints) {
            queueForApproval(envContext.deliveryConfig, envContext.artifact, artifactVersion.version, envContext.environment.name)
          }
        }
      }
  }

  /**
   * A container to hold all the context we need for evaluating environment constraints
   */
  data class EnvironmentContext(
    val deliveryConfig: DeliveryConfig,
    val environment: Environment,
    val artifact: DeliveryArtifact,
    val versions: List<String>,
    val vetoedVersions: Set<String>
  )

  /**
   * Queues a version for approval if it's not already approved in the environment.
   * [EnvironmentPromotionChecker] handles actually approving a version for an environment.
   */
  private fun queueForApproval(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String
  ) {
    val latestVersion = repository.latestVersionApprovedIn(deliveryConfig, artifact, targetEnvironment)
    if (latestVersion != version) {
      log.debug("Queueing version $version of ${artifact.type} artifact ${artifact.name} in environment $targetEnvironment for approval")
      repository.queueArtifactVersionForApproval(deliveryConfig.name, targetEnvironment, artifact, version)
    } else {
      log.debug("Not queueing version $version of $artifact in environment $targetEnvironment for approval as it's already approved")
    }
  }

  suspend fun getStatelessConstraintSnapshots(
    artifact: DeliveryArtifact,
    deliveryConfig: DeliveryConfig,
    version: String,
    environment: Environment,
    currentStatus: ConstraintStatus?
  ): List<ConstraintState> =
    environment.constraints.mapNotNull { constraint ->
      constraint
        .findStatelessEvaluator()
        ?.generateConstraintStateSnapshot(
          artifact = artifact,
          deliveryConfig = deliveryConfig,
          version = version,
          targetEnvironment = environment,
          currentStatus = currentStatus
        )
    }

  suspend fun checkStatelessConstraints(
    artifact: DeliveryArtifact,
    deliveryConfig: DeliveryConfig,
    version: String,
    environment: Environment
  ): Boolean =
    checkConstraintForEveryEnvironment(implicitStatelessEvaluators, artifact, deliveryConfig, version, environment) &&
      checkConstraintWhenSpecified(statelessEvaluators, artifact, deliveryConfig, version, environment)

  suspend fun checkStatefulConstraints(
    artifact: DeliveryArtifact,
    deliveryConfig: DeliveryConfig,
    version: String,
    environment: Environment
  ): Boolean =
    checkConstraintForEveryEnvironment(implicitStatefulEvaluators, artifact, deliveryConfig, version, environment) &&
      checkConstraintWhenSpecified(statefulEvaluators, artifact, deliveryConfig, version, environment)

  /**
   * Checks constraints for a list of evaluators.
   * Evaluates the constraint for every environment passed in.
   * @return true if all constraints pass
   */
  private suspend fun checkConstraintForEveryEnvironment(
    evaluators: List<ApprovalConstraintEvaluator<*>>,
    artifact: DeliveryArtifact,
    deliveryConfig: DeliveryConfig,
    version: String,
    environment: Environment
  ): Boolean =
    evaluators.all { evaluator ->
      evaluator.constraintPasses(artifact, version, deliveryConfig, environment)
    }

  /**
   * Checks constraints for a list of evaluators.
   * Evaluates the constraint only if it's defined on the environment.
   * @return true if all constraints pass
   */
  private suspend fun checkConstraintWhenSpecified(
    evaluators: List<ApprovalConstraintEvaluator<*>>,
    artifact: DeliveryArtifact,
    deliveryConfig: DeliveryConfig,
    version: String,
    environment: Environment
  ): Boolean {
    var allPass = true
    // we want to run all stateful evaluators even if some fail
    evaluators.forEach { evaluator ->
      val canPromote = !environment.hasSupportedConstraint(evaluator) ||
        evaluator.constraintPasses(artifact, version, deliveryConfig, environment)

      if (!canPromote) {
        allPass = false
      }
    }
    return allPass
  }


  private fun Environment.hasSupportedConstraint(constraintEvaluator: ConstraintEvaluator<*>) =
    constraints.any { it.javaClass.isAssignableFrom(constraintEvaluator.supportedType.type) }

  private fun Constraint.findStatelessEvaluator(): StatelessConstraintEvaluator<*,*>? =
    statelessEvaluators.filterIsInstance<StatelessConstraintEvaluator<*, *>>().firstOrNull { javaClass.isAssignableFrom(it.supportedType.type) }
}
