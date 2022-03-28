package com.netflix.spinnaker.keel.api

/**
 * Common interface for [ResourceSpec]s that represent compute resources.
 */
interface ComputeResourceSpec<LOCATIONS: AccountAwareLocations<*>> :
  SpinnakerResourceSpec<LOCATIONS>, DeployableResourceSpec
