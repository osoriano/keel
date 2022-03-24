package com.netflix.spinnaker.keel.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "keel.artifact-refresh")
class ArtifactRefreshConfig {
  var scheduledSyncLimit: Int = 1
  var firstLoadLimit: Int = 20
}
