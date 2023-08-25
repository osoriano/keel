package com.netflix.spinnaker.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

import java.time.Duration

@ConstructorBinding
@ConfigurationProperties(prefix = "keel.artifact-version-cleanup")
data class ArtifactVersionCleanupConfig(
  /* Maximum number of versions to keep */
  val threshold: Int = 2000
)
