package com.netflix.spinnaker.keel.verification

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.BaseActionRunner
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.Verification
import com.netflix.spinnaker.keel.api.action.ActionRepository
import com.netflix.spinnaker.keel.api.action.ActionState
import com.netflix.spinnaker.keel.api.action.ActionType
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PENDING
import com.netflix.spinnaker.keel.api.plugins.VerificationEvaluator
import com.netflix.spinnaker.keel.enforcers.EnvironmentExclusionEnforcer
import com.netflix.spinnaker.keel.telemetry.VerificationCompleted
import com.netflix.spinnaker.keel.telemetry.VerificationStarted
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component

@Component
class VerificationRunner(
  override val actionRepository: ActionRepository,
  private val evaluators: List<VerificationEvaluator<*>>,
  private val eventPublisher: ApplicationEventPublisher,
  private val imageFinder: ImageFinder,
  private val enforcer: EnvironmentExclusionEnforcer,
  override val spectator: Registry
): BaseActionRunner<Verification>() {
  override fun logSubject() = ActionType.VERIFICATION.name

  override fun ArtifactInEnvironmentContext.getActions(): List<Verification> =
    environment.verifyWith

  /**
   * Verifications are run one at a time
   */
  override fun runInSeries() = true

  override fun actionBlocked(context: ArtifactInEnvironmentContext): Boolean {
    return false
  }

  override suspend fun start(context: ArtifactInEnvironmentContext, action: Verification) {
    enforcer.withVerificationLease(context) {
      log.debug("Starting verification for ${context.shortName()}")
      val images = imageFinder.getImages(context.deliveryConfig, context.environmentName)
      val metadata = evaluators.start(context, action) + mapOf("images" to images)
      actionRepository.updateState(context, action, PENDING, metadata)
    }
  }

  override suspend fun evaluate(
    context: ArtifactInEnvironmentContext,
    action: Verification,
    oldState: ActionState
  ): ActionState =
    evaluators.evaluatorFor(action).evaluate(context, action, oldState)

  override fun publishCompleteEvent(context: ArtifactInEnvironmentContext, action: Verification, state: ActionState) {
    eventPublisher.publishEvent(VerificationCompleted(context, action, state.status, state.metadata))
  }

  override fun publishStartEvent(context: ArtifactInEnvironmentContext, action: Verification) {
    eventPublisher.publishEvent(VerificationStarted(context, action))
  }

  private fun <V : Verification> List<VerificationEvaluator<*>>.evaluatorFor(verification: V) =
    first { it.supportedVerification.first == verification.type } as VerificationEvaluator<V>

  private suspend fun <V : Verification> List<VerificationEvaluator<*>>.start(context: ArtifactInEnvironmentContext, verification: V) =
    evaluatorFor(verification).start(context, verification)

  private val log by lazy { LoggerFactory.getLogger(javaClass) }
}
