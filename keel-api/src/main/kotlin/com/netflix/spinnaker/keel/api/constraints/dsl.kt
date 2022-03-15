package com.netflix.spinnaker.keel.api.constraints

/**
 * TODO: Docs.
 */
val List<ConstraintState>.allPass: Boolean
  get() = all { it.status.passes() }
