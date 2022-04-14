package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.core.api.PromotionStatus
import java.time.Instant

/**
 * Holds the info we need about artifacts in an environment for building the UI view.
 *
 * This is used in a list of versions pertaining to a specific delivery artifact.
 */
data class StatusInfoForArtifactInEnvironment(
  val version: String,
  val status: PromotionStatus,
  val replacedByVersion: String?,
  val deployedAt: Instant
)
