package com.netflix.spinnaker.keel.diff

import de.danielbechler.diff.identity.IdentityService

/**
 * Implement and register as a spring bean in order to customize the [IdentityService] of the differ.
 */
interface IdentityServiceCustomizer {
  fun customize(identityService: IdentityService)
}
