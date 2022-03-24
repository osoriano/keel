package com.netflix.spinnaker.keel.api

interface Locations<T : RegionSpec> {
  val account: String
  val regions: Set<T>
}

interface SimpleLocationProvider {
  val account: String
  val region: String
}
