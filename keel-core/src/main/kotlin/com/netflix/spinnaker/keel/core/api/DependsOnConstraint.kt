package com.netflix.spinnaker.keel.core.api

import com.netflix.spinnaker.keel.api.Constraint
import java.time.Duration

/**
 * A constraint that requires that an artifact be successfully deployed to a previous
 * [environment] first, and optionally enforces a delay to [deployAfter] a specified
 * duration.
 */
data class DependsOnConstraint(
  val environment: String,
  val deployAfter: Duration = Duration.ZERO
) : Constraint("depends-on")
