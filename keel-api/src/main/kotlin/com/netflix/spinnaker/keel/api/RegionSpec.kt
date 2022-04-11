package com.netflix.spinnaker.keel.api

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY

interface RegionSpec {
  val name: String
}

data class SubnetAwareRegionSpec(
  override val name: String,
  /**
   * If empty this implies the resource should use _all_ availability zones.
   */
  @get:JsonInclude(NON_EMPTY)
  val availabilityZones: Set<String> = emptySet()
) : RegionSpec

data class SimpleRegionSpec(
  override val name: String
) : RegionSpec {
  override fun toString() = name
}
