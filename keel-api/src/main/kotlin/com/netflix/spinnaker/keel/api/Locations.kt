package com.netflix.spinnaker.keel.api

interface Locations<T : RegionSpec> {
  val regions: Set<T>
}

interface SimpleLocationProvider {
  val account: String
  val region: String
}
