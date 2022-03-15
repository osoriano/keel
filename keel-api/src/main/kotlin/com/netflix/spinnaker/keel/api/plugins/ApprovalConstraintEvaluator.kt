package com.netflix.spinnaker.keel.api.plugins

import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.plugins.ConstraintType.APPROVAL

interface ApprovalConstraintEvaluator<CONSTRAINT: Constraint> : ConstraintEvaluator<CONSTRAINT> {

  override fun constraintType() = APPROVAL

  /**
   * @return true if this is a stateful plugin, false if it's a stateless plugin
   */
  fun isStateful(): Boolean

    /**
   * @return true if the constraint passes, false otherwise.
   */
  suspend fun constraintPasses(
    artifact: DeliveryArtifact,
    version: String,
    deliveryConfig: DeliveryConfig,
    targetEnvironment: Environment
  ): Boolean
}
