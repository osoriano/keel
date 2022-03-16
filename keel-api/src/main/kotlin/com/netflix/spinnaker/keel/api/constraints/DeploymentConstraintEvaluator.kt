package com.netflix.spinnaker.keel.api.constraints

import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.SKIPPED
import com.netflix.spinnaker.keel.api.plugins.ConstraintEvaluator
import com.netflix.spinnaker.keel.api.plugins.ConstraintType.DEPLOYMENT
import java.time.Clock

abstract class DeploymentConstraintEvaluator<CONSTRAINT: Constraint, ATTRIBUTES : ConstraintStateAttributes>(
  open val repository: ConstraintRepository,
  open val clock: Clock
) : ConstraintEvaluator<CONSTRAINT> {

  override fun constraintType() = DEPLOYMENT

  /**
   * The type of the metadata saved about the constraint, surfaced here to automatically register it
   * for serialization
   */
  abstract val attributeType: SupportedConstraintAttributesType<ATTRIBUTES>

  /**
   * @return true if the version can be deployed, false otherwise.
   */
  suspend fun canDeploy(
    artifact: DeliveryArtifact,
    version: String,
    deliveryConfig: DeliveryConfig,
    targetEnvironment: Environment
  ): Boolean {
    val constraint = ConstraintEvaluator.getConstraintForEnvironment(
      deliveryConfig,
      targetEnvironment.name,
      supportedType.type
    )

    var state = repository.getCurrentState(artifact, version, deliveryConfig, targetEnvironment, constraint)

    val newState = when {
      state.judgedByUser() -> state // keep what the user set
      !shouldAlwaysReevaluate() && state.complete() -> state // use existing completed state, we shouldn't reevaluate
      else -> {
        // let the constraint reevaluate and tell us the new state
        calculateConstraintState(
          artifact = artifact,
          version = version,
          deliveryConfig = deliveryConfig,
          targetEnvironment = targetEnvironment,
          constraint = constraint,
          state = state
        )
      }
    }
    repository.storeConstraintState(newState)
    return newState.passed()
  }

  fun markVersionAsSkipped(
    artifact: DeliveryArtifact,
    version: String,
    deliveryConfig: DeliveryConfig,
    targetEnvironment: Environment
  ) {
    val constraint = ConstraintEvaluator.getConstraintForEnvironment(
      deliveryConfig,
      targetEnvironment.name,
      supportedType.type
    )

    val state = repository.getCurrentState(artifact, version, deliveryConfig, targetEnvironment, constraint)
    repository.storeConstraintState(
      state.copy(
        status = SKIPPED,
        comment = "No longer evaluating constraint because a newer version was picked for deployment",
        judgedAt = clock.instant()
      )
    )
  }

  /**
   * This function is called if a user has not judged the constraint
   * (if the state is not OVERRIDE_FAIL or OVERRIDE_PASS) .
   *
   * @return the current status of this constraint.
   */
  abstract suspend fun calculateConstraintState(
    artifact: DeliveryArtifact,
    version: String,
    deliveryConfig: DeliveryConfig,
    targetEnvironment: Environment,
    constraint: CONSTRAINT,
    state: ConstraintState
  ): ConstraintState

  /**
   * Controls whether we should ask the constraint plugin to decide the result every time, or just when the
   * status is not a terminal status.
   *
   * Return true to allow this constraint to be able to flip from pass to fail.
   * Otherwise, once the constraint enters a pass/fail state, it stays there forever.
   */
  open fun shouldAlwaysReevaluate(): Boolean = false
}
