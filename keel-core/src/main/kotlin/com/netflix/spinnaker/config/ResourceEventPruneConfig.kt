package com.netflix.spinnaker.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "keel.resource.prune")
class ResourceEventPruneConfig {
  var minEventsKept: Int = 100
  var deleteChunkSize: Int = 100
  var daysKept: Long = 14L
}
