package com.netflix.spinnaker.keel.constraints

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.constraints.ConstraintRepository
import com.netflix.spinnaker.keel.api.constraints.ConstraintState
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.api.constraints.StatefulConstraintEvaluator
import com.netflix.spinnaker.keel.api.constraints.SupportedConstraintAttributesType
import com.netflix.spinnaker.keel.api.constraints.SupportedConstraintType
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.core.api.ManualJudgementConstraint
import java.time.Clock
import org.springframework.stereotype.Component

@JvmDefaultWithoutCompatibility  // see https://youtrack.jetbrains.com/issue/KT-39603
@Component
class ManualJudgementConstraintEvaluator(
  override val repository: ConstraintRepository,
  private val clock: Clock,
  override val eventPublisher: EventPublisher
) : StatefulConstraintEvaluator<ManualJudgementConstraint, ManualJudgementConstraintAttributes> {

  override val supportedType = SupportedConstraintType<ManualJudgementConstraint>("manual-judgement")
  override val attributeType = SupportedConstraintAttributesType<ManualJudgementConstraintAttributes>("manual-judgement")

  override suspend fun constraintPasses(
    artifact: DeliveryArtifact,
    version: String,
    deliveryConfig: DeliveryConfig,
    targetEnvironment: Environment,
    constraint: ManualJudgementConstraint,
    state: ConstraintState
  ): Boolean {
    if (state.failed()) {
      return false
    }

    if (state.timedOut(constraint.timeout, clock.instant())) {
      repository
        .storeConstraintState(
          state.copy(
            status = ConstraintStatus.FAIL,
            comment = "Timed out after ${constraint.timeout}"
          )
        )

      // TODO: Emit event
      return false
    }

    return state.passed()
  }
}
