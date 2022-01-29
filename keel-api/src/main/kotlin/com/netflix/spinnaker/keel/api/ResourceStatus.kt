package com.netflix.spinnaker.keel.api

import java.time.Instant

enum class ResourceStatus {
  CREATED,
  DIFF,
  ACTUATING,
  HAPPY,
  UNHAPPY,
  MISSING_DEPENDENCY,
  CURRENTLY_UNRESOLVABLE,
  ERROR,
  PAUSED,
  RESUMED,
  UNKNOWN,
  DIFF_NOT_ACTIONABLE,
  WAITING,
  DELETING
}

data class ResourceStatusSnapshot(
  val status: ResourceStatus,
  val updatedAt: Instant
)
