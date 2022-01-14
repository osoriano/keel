package com.netflix.spinnaker.keel.core.api

import java.time.Instant

/**
 * Snapshot of the actions Keel would take on resources associated with an application at a given point in time.
 */
data class ActuationPlan(
  val application: String,
  val timestamp: Instant,
  val environmentPlans: List<EnvironmentPlan>
)

data class EnvironmentPlan(
  val environment: String,
  val resourcePlans: List<ResourcePlan>
)

data class ResourcePlan(
  val resourceId: String,
  val action: ResourceAction,
  val diff: Map<String, Any?> = emptyMap()
)

enum class ResourceAction {
  NONE, CREATE, UPDATE
}
