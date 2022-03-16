package com.netflix.spinnaker.keel.constraints

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.config.FeatureToggles.Companion.ON_OFF_CONSTRAINT
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.constraints.ConstraintRepository
import com.netflix.spinnaker.keel.api.constraints.ConstraintState
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.FAIL
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PASS
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PENDING
import com.netflix.spinnaker.keel.api.constraints.DefaultConstraintAttributes
import com.netflix.spinnaker.keel.api.constraints.SupportedConstraintAttributesType
import com.netflix.spinnaker.keel.api.constraints.SupportedConstraintType
import com.netflix.spinnaker.keel.api.constraints.DeploymentConstraintEvaluator
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.core.api.OnOffConstraint
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Clock

/**
 * A constraint that gates the deployment of an artifact (not the approval of an artifact)
 * by checking a fast property to see if we can allow the deploy.
 *
 * Used for testing.
 */
@Component
class OnOffDeploymentConstraintEvaluator(
  override val repository: ConstraintRepository,
  override val eventPublisher: EventPublisher,
  val featureToggles: FeatureToggles,
  override val clock: Clock
): DeploymentConstraintEvaluator<OnOffConstraint, DefaultConstraintAttributes>(repository, clock) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  override val supportedType: SupportedConstraintType<OnOffConstraint> = SupportedConstraintType<OnOffConstraint>("on-off")

  override val attributeType: SupportedConstraintAttributesType<DefaultConstraintAttributes>
    = SupportedConstraintAttributesType<DefaultConstraintAttributes>("on-off")

  override suspend fun calculateConstraintState(
    artifact: DeliveryArtifact,
    version: String,
    deliveryConfig: DeliveryConfig,
    targetEnvironment: Environment,
    constraint: OnOffConstraint,
    state: ConstraintState
  ): ConstraintState {
    val canDeploy = featureToggles.isEnabled(ON_OFF_CONSTRAINT, false)
    log.debug("on-off constraint says $canDeploy for version $version in environment $targetEnvironment and application ${deliveryConfig.application}")

    return state.copy(
      status = if (canDeploy) PASS else PENDING,
      judgedAt = clock.instant(),
      judgedBy = "Spinnaker",
      comment = if (canDeploy) null else "Cannot deploy this version because deployments are off"
    )
  }

  override fun shouldAlwaysReevaluate(): Boolean = true
}
