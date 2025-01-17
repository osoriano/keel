package com.netflix.spinnaker.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "keel.task-check")
class TaskCheckConfig: BaseSchedulerConfig() {
  // only uses properties from the BaseSchedulerConfig,
  // but this is here to give a separate prefix for overriding the values
  // via fast property or in the config file.
}
