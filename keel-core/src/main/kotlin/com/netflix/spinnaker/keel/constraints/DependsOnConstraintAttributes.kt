package com.netflix.spinnaker.keel.constraints

import com.netflix.spinnaker.keel.api.constraints.ConstraintStateAttributes
import java.time.Duration

data class DependsOnConstraintAttributes(
  val dependsOnEnvironment: String,
  val currentlyPassing: Boolean = true,
  val deployAfter: Duration = Duration.ZERO
) : ConstraintStateAttributes("depends-on")
