@file:Suppress("MemberVisibilityCanBePrivate")

package com.netflix.spinnaker.keel.titus

import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.SimpleLocations
import com.netflix.spinnaker.keel.api.SimpleRegionSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterDependencies
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.titus.NetworkMode
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.api.titus.TitusServerGroupSpec
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.clouddriver.model.SecurityGroupSummary
import com.netflix.spinnaker.keel.core.api.DEFAULT_SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.docker.DigestProvider
import io.mockk.coEvery as every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.core.env.Environment
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isNull

internal class NetworkModeResolverTests {

  val springEnv: Environment = mockk(relaxed = true) {
    every {
      getProperty("keel.titus.resolvers.network-mode.enabled", Boolean::class.java, true)
    } returns true
  }

  val cloudDriverService = mockk<CloudDriverService>() {
    every { getAccountInformation(any(), any()) } returns mapOf("environment" to "unknown")
  }

  val resolver = NetworkModeResolver(DefaultContainerAttributes(), springEnv, cloudDriverService)

  @Test
  fun `networkMode takes over precedence container attribute`() {
    // container attribute says no IPv6 (i.e., IPv4 only), networkMode says IPv6 only
    val original = resource(assignIPv6Address="false", networkMode=NetworkMode.Ipv6Only)

    val resolved = resolver.invoke(original)

    val networkMode = resolved.spec.defaults.networkMode

    // networkMode should win
    expectThat(networkMode.toString()).isEqualTo("Ipv6Only")
  }

  @Test
  fun `use dual stack`() {
    val original = resource(assignIPv6Address="true")

    val resolved = resolver.invoke(original)

    val networkMode = resolved.spec.defaults.networkMode
    expectThat(networkMode.toString()).isEqualTo("Ipv6AndIpv4")
  }

  @Test
  fun `use IPv4 only`() {
    val original = resource(assignIPv6Address="false")

    val resolved = resolver.invoke(original)

    val networkMode = resolved.spec.defaults.networkMode
    expectThat(networkMode.toString()).isEqualTo("Ipv4Only")
  }

  @Test
  fun `default to dual stack if unspecified and in test environment`() {
    every { cloudDriverService.getAccountInformation(any(), any()) } returns mapOf("environment" to "test")

    val original = resource(assignIPv6Address=null)

    val resolved = resolver.invoke(original)

    val networkMode = resolved.spec.defaults.networkMode
    expectThat(networkMode.toString()).isEqualTo("Ipv6AndIpv4")
  }

  @Test
  fun `default to IPv4 if unspecified and non-test account`() {

    every { cloudDriverService.getAccountInformation(any(), any()) } returns mapOf("environment" to "prod")

    val original = resource(assignIPv6Address=null)

    val resolved = resolver.invoke(original)

    val networkMode = resolved.spec.defaults.networkMode
    expectThat(networkMode.toString()).isEqualTo("Ipv4Only")
  }

  @Test
  fun `respect regional overrides`() {
    val overrides = mapOf(
      "us-east-1" to TitusServerGroupSpec(containerAttributes = mapOf("titusParameter.agent.assignIPv6Address" to "true")),
      "us-west-2" to TitusServerGroupSpec(containerAttributes = mapOf("titusParameter.agent.assignIPv6Address" to "false")),
    )

    val original = resource(assignIPv6Address = null, overrides = overrides)
    val resolved = resolver.invoke(original)

    expectThat(resolved.spec.overrides["us-east-1"]?.networkMode?.toString())
      .isEqualTo("Ipv6AndIpv4")

    expectThat(resolved.spec.overrides["us-west-2"]?.networkMode?.toString())
      .isEqualTo("Ipv4Only")
  }


  fun resource(assignIPv6Address: String?, overrides: Map<String, TitusServerGroupSpec> = emptyMap(), networkMode: NetworkMode? = null) : Resource<TitusClusterSpec> {

    val containerAttributes = assignIPv6Address?.let {
      mapOf( "titusParameter.agent.assignIPv6Address" to it)
    } ?: emptyMap()

    return Resource(
      kind = TITUS_CLUSTER_V1.kind,
      metadata = mapOf("id" to "id", "application" to "keel"),
      spec = TitusClusterSpec(
        moniker = Moniker(app = "keel", stack = "test"),
        locations = SimpleLocations(
          account = "titustest",
          regions = setOf(SimpleRegionSpec("us-east-1"), SimpleRegionSpec("us-west-2"), SimpleRegionSpec("eu-west-1"))
        ),
        _defaults = TitusServerGroupSpec(
          capacity = ClusterSpec.CapacitySpec(1, 6, 4),
          containerAttributes = containerAttributes,
          networkMode = networkMode,
          dependencies = ClusterDependencies(
            loadBalancerNames = setOf("keel-test-frontend"),
            securityGroupNames = setOf(SecurityGroupSummary("keel", "sg-325234532", "vpc-1").name)
          )
        ),
        overrides = overrides,
        container = DigestProvider(
          organization = "spinnaker",
          image = "keel",
          digest = "sha:1111"
        )
      )
    )
  }


}
