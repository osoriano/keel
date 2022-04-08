package com.netflix.spinnaker.config

import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.stereotype.Component

/**
 * Simple wrapper around the Spring environment to retrieve feature toggles dynamically.
 */
@Component
class FeatureToggles(private val springEnvironment: ConfigurableEnvironment) {
  companion object {
    // Feature names that can be used with FeatureToggles
    const val OPTIMIZED_DOCKER_FLOW: String = "keel.artifacts.optimized-docker-flow"
    const val GRAPHQL_SCHEMA_VALIDATION_CONSTRAINT: String = "keel.constraints.schema-validation.enabled"
    const val COROUTINE_MONITORING: String = "keel.metrics.coroutines.enabled"
    const val SKIP_PAUSED_APPS: String = "keel.environment-check.skip-paused-apps"
    const val USE_READ_REPLICA: String = "read-replica.enabled"
  }

  /**
   * @return the boolean value of the specified [feature] from the [springEnvironment], or the [default]
   * if the corresponding configuration property is not set.
   */
  fun isEnabled(feature: String, default: Boolean = true): Boolean =
    springEnvironment.getProperty(feature, Boolean::class.java, default)
}
