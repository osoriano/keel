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
    configOrDefault("check-interval", sanitizeKindForFP(request.resourceKind), Duration.ofMinutes(1))

  override fun getEnvironmentCheckInterval(): Duration =
    environment.getProperty(
      "keel.environment-scheduler.check-interval",
      Duration::class.java,
      Duration.ofMinutes(1)
    )

  override fun getContinueAsNewInterval(request: CheckResourceKindRequest): Duration =
    configOrDefault("continue-as-new-interval", sanitizeKindForFP(request.resourceKind), Duration.ofHours(24))

  /**
   * Fast properties can't have any of the symbols.
   * A property to change the check-interval for graphql resources, for example, should have a key of
   *
   * keel.resource-scheduler.graphql-schema-v1.check-interval
   */
  fun sanitizeKindForFP(kind: String): String =
    kind.replace("/", "-").replace("@", "-")

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
