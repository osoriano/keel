package com.netflix.spinnaker.keel.api.events

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.ArtifactType
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import java.time.Instant

/**
 * An event that conveys information about one or more [PublishedArtifact] that are
 * potentially relevant to keel.
 */
data class ArtifactPublishedEvent(
  val artifacts: List<PublishedArtifact>,
  val details: Map<String, Any>? = emptyMap()
)

/**
 * Event emitted with a new [DeliveryArtifact] is registered.
 */
data class ArtifactRegisteredEvent(
  val artifact: DeliveryArtifact
)

/**
 * Event emitted to trigger synchronization of artifact information.
 */
data class AllArtifactsSyncEvent(
  val controllerTriggered: Boolean = false
)

/**
 * Event emitted to trigger synchronization of artifact information.
 */
data class ArtifactSyncEvent(
  val application: String,
  val artifactReference: String,
  val limit: Int
)

/**
 * An event fired when an artifact version is stored in the database.
 */
data class ArtifactVersionStored(
  val publishedArtifact: PublishedArtifact
)

/**
 * An event fired to signal that an artifact version is deploying to a resource.
 */
data class ArtifactVersionDeploying(
  val resourceId: String,
  val artifactVersion: String
)

/**
 * An event fired when an artifact version is marked as deploying in the database.
 */
data class ArtifactVersionMarkedDeploying(
  val deliveryConfig: DeliveryConfig,
  val artifact: DeliveryArtifact,
  val version: String,
  val environment: Environment,
  val timestamp: Instant
)

/**
 * An event fired to signal that an artifact version was successfully deployed to a resource.
 */
data class ArtifactVersionDeployed(
  val resourceId: String,
  val artifactVersion: String
)

/**
 * An event fired to signal that a new artifact version was first detected by Keel.
 */
data class ArtifactVersionDetected(
  val deliveryConfig: DeliveryConfig,
  val artifact: DeliveryArtifact,
  val version: PublishedArtifact
)
