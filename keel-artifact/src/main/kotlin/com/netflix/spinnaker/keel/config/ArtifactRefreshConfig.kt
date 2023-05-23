package com.netflix.spinnaker.keel.config

import com.netflix.spinnaker.config.BaseSchedulerConfig
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "keel.artifact-refresh")
class ArtifactRefreshConfig: BaseSchedulerConfig() {
  var limit: Int = 1
}
