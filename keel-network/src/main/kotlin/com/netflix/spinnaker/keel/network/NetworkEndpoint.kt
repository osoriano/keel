package com.netflix.spinnaker.keel.network

import com.netflix.spinnaker.keel.network.NetworkEndpointType.EUREKA_CLUSTER_DNS
import com.netflix.spinnaker.keel.network.NetworkEndpointType.EUREKA_GRPC_VIP_DNS
import com.netflix.spinnaker.keel.network.NetworkEndpointType.EUREKA_VIP_DNS

/**
 * A network endpoint one can connect to on a [Resource].
 *
 * @see NetworkEndpointProvider
 */
data class NetworkEndpoint(
  val type: NetworkEndpointType,
  val region: String,
  val address: String
) {
  val protocol: String = when(type) {
    EUREKA_CLUSTER_DNS, EUREKA_VIP_DNS -> if ("-secure" in address) "https" else "http"
    EUREKA_GRPC_VIP_DNS -> "grpc"
    else -> "http"
  }
}

enum class NetworkEndpointType(
  val friendlyName: String
) {
  EUREKA_VIP_DNS("Eureka VIP"),
  EUREKA_CLUSTER_DNS("Eureka cluster name"),
  EUREKA_GRPC_VIP_DNS("Eureka gRPC VIP"),
  DNS("Hostname"),
  IPV4("IPv4 address"),
  IPV6("IPv6 address");
}
