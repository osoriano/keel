package com.netflix.spinnaker.keel.network

import com.netflix.spinnaker.config.DnsConfig
import com.netflix.spinnaker.keel.api.ComputeResourceSpec
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.ec2.LoadBalancerSpec
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.core.api.DEFAULT_SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.network.NetworkEndpointType.DNS
import com.netflix.spinnaker.keel.network.NetworkEndpointType.EUREKA_CLUSTER_DNS
import com.netflix.spinnaker.keel.network.NetworkEndpointType.EUREKA_VIP_DNS
import kotlinx.coroutines.runBlocking
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.stereotype.Component

/**
 * Provides network endpoints for certain resource types based on configuration and runtime information
 * available from CloudDriver.
 */
@Component
@EnableConfigurationProperties(DnsConfig::class)
class NetworkEndpointProvider(
  private val cloudDriverCache: CloudDriverCache,
  private val cloudDriverService: CloudDriverService,
  private val dnsConfig: DnsConfig
) {
  suspend fun getNetworkEndpoints(resource: Resource<*>, forPreviewEnvironment: Boolean = false): Set<NetworkEndpoint> {
    with(resource.spec) {
      return when (this) {
        is ComputeResourceSpec<*> -> {
          locations.regions.flatMap { region ->
            listOf(
              // Example: lpollolocaltest-preview-5bd0005.vip.us-east-1.test.acme.net
              NetworkEndpoint(EUREKA_VIP_DNS, region.name, "${moniker.toVip(forPreviewEnvironment)}.vip.${region.name}.${locations.account.environment}.${dnsConfig.defaultDomain}"),
              // Example: lpollolocaltest-preview-5bd0005.cluster.us-east-1.test.acme.net
              NetworkEndpoint(EUREKA_CLUSTER_DNS, region.name, "${moniker.toName()}.cluster.${region.name}.${locations.account.environment}.${dnsConfig.defaultDomain}"),
            )
          }.toSet()
        }
        is LoadBalancerSpec -> {
          locations.regions.mapNotNull { region ->
            // Example: internal-keel-test-vpc0-1234567890.us-west-2.elb.amazonaws.com
            cloudDriverService.getAmazonLoadBalancer(
              user = DEFAULT_SERVICE_ACCOUNT,
              account = locations.account,
              region = region.name,
              name = moniker.toName()
            ).firstOrNull()
              ?.let { NetworkEndpoint(DNS, region.name, it.dnsName) }
          }.toSet()
        }
        else -> emptySet()
      }
    }
  }

  private val String.environment: String
    get() = runBlocking {
      cloudDriverCache.credentialBy(this@environment).environment
    }
}

fun Moniker.toVip(forPreviewEnvironment: Boolean = false): String =
  when(forPreviewEnvironment) {
    // for preview environments, we must include the detail since the point is to isolate traffic
    true -> app + (if (stack != null) "-$stack" else "") + (if (detail != null) "-$detail" else "")
    // otherwise, the default is to use the app and stack with no dashes
    false -> app + if (stack != null) stack else ""
  }

fun Moniker.toSecureVip(): String =
  toVip() + "-secure"
