package com.netflix.spinnaker.keel.network

/**
 * A network endpoint one can connect to on a [Resource].
 *
 * @see NetworkEndpointProvider
 */
data class NetworkEndpoint(
  val type: NetworkEndpointType,
  val region: String,
  val address: String
)

enum class NetworkEndpointType(
  val friendlyName: String
) {
  EUREKA_VIP_DNS("Eureka DNS VIP"),
  EUREKA_CLUSTER_DNS("Eureka DNS name"),
  DNS("Hostname"),
  IPV4("IPv4 address"),
  IPV6("IPv6 address");
}
