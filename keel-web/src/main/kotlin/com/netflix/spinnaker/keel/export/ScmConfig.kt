package com.netflix.spinnaker.keel.export

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "keel.pipelines-export.scm")
class ScmConfig {
  var jobType: List<String>? = null
}
