package com.netflix.spinnaker.keel.constraints

import com.netflix.spinnaker.keel.api.plugins.ApprovalConstraintEvaluator
import com.netflix.spinnaker.keel.api.plugins.ConstraintEvaluator
import com.netflix.spinnaker.keel.api.plugins.ConstraintType.APPROVAL
import org.springframework.context.annotation.Lazy
import org.springframework.stereotype.Component

/**
 * A container to hold all of the constraint evaluators
 */
@Lazy
@Component
class ConstraintEvaluators(
  val constraints: List<ConstraintEvaluator<*>>
) {

  fun allEvaluators() =
    constraints

  fun approvalEvaluators() =
    constraints.filter { it.constraintType() == APPROVAL } as List<ApprovalConstraintEvaluator<*>>
}
