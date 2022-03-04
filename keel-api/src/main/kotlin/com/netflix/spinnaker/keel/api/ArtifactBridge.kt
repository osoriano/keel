package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.keel.api.artifacts.ArtifactMetadata


/**
 * This is a bridge to calling Igor for artifact related endpoints.
 */
interface ArtifactBridge {
  companion object {
    private const val DEFAULT_MAX_ATTEMPTS = 20
  }

  suspend fun getArtifactMetadata(
    buildNumber: String,
    commitHash: String,
    maxAttempts: Int = DEFAULT_MAX_ATTEMPTS
  ): ArtifactMetadata?

}
