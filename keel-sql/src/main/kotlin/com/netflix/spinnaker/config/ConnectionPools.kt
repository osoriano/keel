package com.netflix.spinnaker.config

enum class ConnectionPools(
  val value: String
) {
  DEFAULT("default"),
  READ_ONLY("readonly"),
}
