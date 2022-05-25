package com.netflix.spinnaker.config

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Copycat of netflixplatform's [DirectCallerAuthorizationProperties] to work around some stupid Spring bean
 * instantiation ordering issue.
 */
@ConfigurationProperties("netflix.sso.direct-caller-authorization-filter")
class KeelDirectCallerAuthorizationProperties {
  var enabled = false
  var exclude: List<Endpoint> = ArrayList()

  class Endpoint {
    var method: String = "fill-me-in"
    var path: String = "fill-me-in"
  }
}
