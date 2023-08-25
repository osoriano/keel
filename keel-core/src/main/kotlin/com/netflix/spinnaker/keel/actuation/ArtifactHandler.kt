package com.netflix.spinnaker.keel.actuation

import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact

interface ArtifactHandler {
  val name: String
    get() = javaClass.simpleName

  suspend fun handle(artifact: DeliveryArtifact)
}

/**
 * Applies all [ArtifactHandler] instances in this collection to [artifact].
 */
suspend fun Collection<ArtifactHandler>.applyAll(artifact: DeliveryArtifact) =
  forEach { it.handle(artifact) }
