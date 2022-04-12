package com.netflix.spinnaker.keel.network

import com.netflix.spinnaker.config.DnsConfig
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.SubnetAwareRegionSpec
import com.netflix.spinnaker.keel.api.ec2.LoadBalancerDependencies
import com.netflix.spinnaker.keel.api.ec2.LoadBalancerSpec
import com.netflix.spinnaker.keel.api.ec2.LoadBalancerType.CLASSIC
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.clouddriver.model.ClassicLoadBalancerModel
import com.netflix.spinnaker.keel.clouddriver.model.ClassicLoadBalancerModel.ClassicLoadBalancerHealthCheck
import com.netflix.spinnaker.keel.clouddriver.model.Credential
import com.netflix.spinnaker.keel.network.NetworkEndpointType.DNS
import com.netflix.spinnaker.keel.network.NetworkEndpointType.EUREKA_CLUSTER_DNS
import com.netflix.spinnaker.keel.network.NetworkEndpointType.EUREKA_VIP_DNS
import com.netflix.spinnaker.keel.test.TEST_API_V1
import com.netflix.spinnaker.keel.test.computeResource
import com.netflix.spinnaker.keel.test.resource
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.isEqualTo
import java.time.Duration
import io.mockk.coEvery as every

class NetworkEndpointProviderTests : JUnit5Minutests {
  class Fixture {
    val cloudDriverCache: CloudDriverCache = mockk()
    val cloudDriverService: CloudDriverService = mockk()
    val dnsConfig = DnsConfig("acme.net")
    val computeResource = computeResource()
    val loadBalancerResource: Resource<LoadBalancerSpec> = resource(
      kind = TEST_API_V1.qualify("loadBalancer"),
      spec = object : LoadBalancerSpec {
        override val loadBalancerType = CLASSIC
        override val locations = SubnetAwareLocations("test", "subnet", regions = setOf(SubnetAwareRegionSpec("us-east-1")))
        override val internal = true
        override val dependencies = LoadBalancerDependencies()
        override val idleTimeout = Duration.ZERO
        override val moniker = Moniker("fnord", "dummy", "loadBalancer")
      }
    )
    val subject = NetworkEndpointProvider(cloudDriverCache, cloudDriverService, dnsConfig)
  }

  fun tests() = rootContext<Fixture> {
    fixture { Fixture() }

    context("NetworkEndpointProvider") {
      before {
        every {
          cloudDriverCache.credentialBy("test")
        } returns Credential("test", "aws", "test", mutableMapOf("accountId" to "1234567890"))

        every {
          cloudDriverService.getAmazonLoadBalancer(any(), "test", "us-east-1", loadBalancerResource.spec.moniker.toName())
        } returns listOf(loadBalancerResource.spec.toAmazonLoadBalancer())
      }

      test("generates VIPs correctly for preview environments") {
        mapOf(
          Moniker("app") to "app",
          Moniker("app", "stack") to "app-stack",
          Moniker("app", "stack", "detail") to "app-stack-detail",
          Moniker("app", detail = "detail") to "app--detail"
        ).forEach { (moniker, vip) ->
          expectThat(moniker.toVip(forPreviewEnvironment = true)).isEqualTo(vip)
        }
      }

      test("generates VIPs correctly for default case") {
        mapOf(
          Moniker("app") to "app",
          Moniker("app", "stack") to "appstack",
          Moniker("app", "stack", "detail") to "appstack",
          Moniker("app", detail = "detail") to "app"
        ).forEach { (moniker, vip) ->
          expectThat(moniker.toVip()).isEqualTo(vip)
        }
      }

      test("returns endpoints for compute resource") {
        val endpoints = runBlocking {
          subject.getNetworkEndpoints(computeResource)
        }
        with(computeResource) {
          expectThat(endpoints).contains(
            NetworkEndpoint(EUREKA_VIP_DNS, "us-east-1", "${spec.moniker.toVip()}.vip.us-east-1.test.acme.net"),
            NetworkEndpoint(EUREKA_CLUSTER_DNS, "us-east-1", "${spec.moniker.toName()}.cluster.us-east-1.test.acme.net")
          )
        }
      }

      test("returns endpoints for load balancer") {
        val endpoints = runBlocking {
          subject.getNetworkEndpoints(loadBalancerResource)
        }
        with(loadBalancerResource) {
          expectThat(endpoints).contains(
            NetworkEndpoint(DNS, "us-east-1",  "internal-${spec.moniker.toName()}-123456789.us-east-1.elb.amazonaws.com")
          )
        }
      }
    }
  }

  private fun LoadBalancerSpec.toAmazonLoadBalancer() =
    ClassicLoadBalancerModel(
      moniker = moniker,
      loadBalancerName = moniker.toName(),
      availabilityZones = emptySet(),
      vpcId = "vpc0",
      subnets = emptySet(),
      scheme = "internal",
      dnsName = "internal-${moniker.toName()}-123456789.us-east-1.elb.amazonaws.com",
      idleTimeout = 0,
      securityGroups = emptySet(),
      listenerDescriptions = emptyList(),
      healthCheck = ClassicLoadBalancerHealthCheck("blah", 0, 0, 0, 0)
    )
}
