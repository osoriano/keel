package com.netflix.spinnaker.keel.scheduling.activities

import com.google.common.annotations.VisibleForTesting
import com.netflix.spinnaker.keel.scheduling.activities.SchedulingConfigActivities.CheckResourceKindRequest
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class DefaultSchedulingConfigActivities(
  private val environment: Environment
) : SchedulingConfigActivities {

  override fun getResourceKindCheckInterval(request: CheckResourceKindRequest): Duration =
    configOrDefault("check-interval", request.resourceKind, Duration.ofSeconds(30))

  override fun getContinueAsNewInterval(request: CheckResourceKindRequest): Duration =
    configOrDefault("continue-as-new-interval", request.resourceKind, Duration.ofHours(24))

  @VisibleForTesting
  internal fun configOrDefault(configKey: String, resourceKind: String, fallback: Duration): Duration {
    return environment.getProperty(
      "keel.resource-scheduler.$resourceKind.$configKey",
      Duration::class.java,
      environment.getProperty(
        "keel.resource-scheduler.default.$configKey",
        Duration::class.java,
        fallback
      )
    )
  }
}
