package com.netflix.spinnaker.keel.api.ec2

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonAnyGetter

data class TargetGroupAttributes(
  @get:JsonAlias("stickiness.enabled")
  val stickinessEnabled: Boolean = false,
  @get:JsonAlias("deregistration_delay.timeout_seconds")
  val deregistrationDelay: Int = 300,
  @get:JsonAlias("stickiness.type")
  val stickinessType: String = "lb_cookie",
  @get:JsonAlias("stickiness.lb_cookie.duration_seconds")
  val stickinessDuration: Int = 86400,
  @get:JsonAlias("slow_start.duration_seconds")
  val slowStartDurationSeconds: Int = 0,
  @get:JsonAnyGetter
  val properties: Map<String, Any?> = emptyMap()
)
