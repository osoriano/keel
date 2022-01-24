package com.netflix.spinnaker.config

import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.stereotype.Component

/**
 * Simple wrapper around the Spring environment to retrieve feature toggles dynamically.
 */
@Component
class FeatureToggles(private val springEnvironment: ConfigurableEnvironment) {
  /**
   * @return the boolean value of the specified [feature] from the [springEnvironment], or the [default]
   * if the corresponding configuration property is not set.
   */
  fun isEnabled(feature: String, default: Boolean = true): Boolean =
    springEnvironment.getProperty(feature, Boolean::class.java, default)
}
