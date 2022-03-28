package com.netflix.spinnaker.keel.api

/**
 * A standard resource that is both [Monikered] and [Locatable] in an AWS account.
 */
interface SpinnakerResourceSpec<LOCATIONS: AccountAwareLocations<*>> : ResourceSpec, Monikered, Locatable<LOCATIONS> {
  /**
   * A default _id_ implementation using the account name and [moniker].
   */
  override fun generateId(metadata: Map<String, Any?>): String = "${locations.account}:$moniker"
}
