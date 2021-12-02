package com.netflix.spinnaker.keel.core.api

import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import java.time.Instant

enum class PinType {
  ROLLBACK, // Pin to another version than current
  LOCK; // Pin to the current version
}

data class EnvironmentArtifactPin(
  val targetEnvironment: String,
  val reference: String,
  val version: String,
  val pinnedBy: String?,
  val comment: String?,
  val type: PinType? = null,
)

data class PinnedEnvironment(
  val deliveryConfigName: String,
  val targetEnvironment: String,
  val artifact: DeliveryArtifact,
  val version: String,
  val pinnedBy: String?,
  val pinnedAt: Instant?,
  val comment: String?,
  val type: PinType? = null,
)
