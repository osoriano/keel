package com.netflix.spinnaker.keel.constraints

import com.netflix.spinnaker.keel.api.constraints.ConstraintStateAttributes
import java.time.Duration

data class DependsOnConstraintAttributes(
  val dependsOnEnvironment: String,
  val currentlyPassing: Boolean = true,
  // FIXME: DGS does not register JavaTimeModule which causes serialization of Duration to break
  // val deployAfter: Duration = Duration.ZERO
  val deployAfter: String = "PT0S"
) : ConstraintStateAttributes("depends-on")
