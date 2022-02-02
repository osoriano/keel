package com.netflix.spinnaker.keel.api.artifacts

import java.time.Instant

data class CurrentlyDeployedVersion(
  val version: String,
  val deployedAt: Instant
)
