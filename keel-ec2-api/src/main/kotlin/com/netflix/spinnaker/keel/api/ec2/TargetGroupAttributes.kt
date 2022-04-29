package com.netflix.spinnaker.keel.api.ec2

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonAnyGetter

data class TargetGroupAttributes(
  @get:JsonAlias("stickiness.enabled")
  val stickinessEnabled: Boolean = false,
  @get:JsonAlias("deregistration_delay.timeout_seconds")
  val deregistrationDelay: Int = 300,
  @get:JsonAlias("stickiness.type")
  val stickinessType: String? = if (stickinessEnabled) "lb_cookie" else null,
  @get:JsonAlias("stickiness.lb_cookie.duration_seconds")
  val stickinessDuration: Int? = if (stickinessEnabled) 86400 else null,
  @get:JsonAlias("slow_start.duration_seconds")
  val slowStartDurationSeconds: Int = 0,
  @get:JsonAnyGetter
  val properties: Map<String, Any?> = emptyMap()
) {
  // FIXME [LFP 4/29/2022]: the following should be enforced, but we have data in the DB that fails the check, and I'm not
  //  sure about CloudDriver responses.
  /*
  init {
    require(stickinessEnabled || (stickinessType == null && stickinessDuration == null)) {
      "Cannot set stickinessType or stickinessDuration if stickinessEnabled is false"
    }
  }
  */
}
