package com.netflix.spinnaker.keel.api.deserialization

/**
 * Supports manipulation of a [SubmittedEnvironment] prior to deserialization.
 */
interface SubmittedEnvironmentPreProcessor {
  /**
   * @param environment The raw submitted environment, may be manipulated in-place.
   */
  fun process(environment: MutableMap<String, Any>)
}
