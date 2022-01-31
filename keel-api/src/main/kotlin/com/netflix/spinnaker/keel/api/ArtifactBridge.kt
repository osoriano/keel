package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.keel.api.artifacts.ArtifactMetadata


/**
 * This is a bridge to calling Igor for artifact related endpoints.
 */
interface ArtifactBridge {

  suspend fun getArtifactMetadata(
    buildNumber: String,
    commitHash: String
  ): ArtifactMetadata?

}
