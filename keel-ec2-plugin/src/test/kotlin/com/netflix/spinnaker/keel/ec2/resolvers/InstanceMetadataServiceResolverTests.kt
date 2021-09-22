package com.netflix.spinnaker.keel.ec2.resolvers

import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.SubnetAwareRegionSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.ec2.EC2_CLUSTER_V1_1
import com.netflix.spinnaker.keel.api.ec2.InstanceMetadataServiceVersion
import com.netflix.spinnaker.keel.api.ec2.InstanceMetadataServiceVersion.V1
import com.netflix.spinnaker.keel.api.ec2.InstanceMetadataServiceVersion.V2
import com.netflix.spinnaker.keel.api.ec2.LaunchConfigurationSpec
import com.netflix.spinnaker.keel.api.ec2.Location
import com.netflix.spinnaker.keel.api.ec2.ServerGroup
import com.netflix.spinnaker.keel.api.ec2.ServerGroup.LaunchConfiguration
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.ec2.resolvers.InstanceMetadataServiceResolver.Companion.featureName
import com.netflix.spinnaker.keel.environments.DependentEnvironmentFinder
import com.netflix.spinnaker.keel.events.ResourceState.Diff
import com.netflix.spinnaker.keel.events.ResourceState.Ok
import com.netflix.spinnaker.keel.persistence.FeatureRolloutRepository
import com.netflix.spinnaker.keel.rollout.FeatureRolloutAttempted
import com.netflix.spinnaker.keel.rollout.FeatureRolloutFailed
import com.netflix.spinnaker.keel.test.resource
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.core.env.Environment
import strikt.api.*
import strikt.assertions.*
import io.mockk.coEvery as every

internal class InstanceMetadataServiceResolverTests {
  private val dependentEnvironmentFinder: DependentEnvironmentFinder = mockk()
  private val resourceToCurrentState: suspend (Resource<ClusterSpec>) -> Map<String, ServerGroup> = mockk()
  private val featureRolloutRepository: FeatureRolloutRepository = mockk(relaxUnitFun = true) {
    every { countRolloutAttempts(any(), any()) } returns 0
  }
  private val eventPublisher: EventPublisher = mockk(relaxUnitFun = true)
  private val springEnvironment: Environment = mockk() {
    // assume the stop rollout flag is not set / default
    every { getProperty("keel.rollout.imdsv2.stopOnFailure", Boolean::class.java, false) } returns false
  }
  private val resolver = InstanceMetadataServiceResolver(
    dependentEnvironmentFinder,
    resourceToCurrentState,
    featureRolloutRepository,
    eventPublisher,
    springEnvironment
  )

  private val spec = ClusterSpec(
    moniker = Moniker(
      app = "fnord"
    ),
    locations = SubnetAwareLocations(
      account = "prod",
      subnet = "internal",
      regions = setOf(
        SubnetAwareRegionSpec(name = "us-west-2")
      )
    )
  )

  private val previousEnvironmentSpec = ClusterSpec(
    moniker = Moniker(
      app = "fnord",
      stack = "test"
    ),
    locations = SubnetAwareLocations(
      account = "test",
      subnet = "internal",
      regions = setOf(
        SubnetAwareRegionSpec(name = "us-west-2")
      )
    )
  )

  @Test
  fun `defaults a cluster to IMDS v2 if not specified and there are no previous environments`() {
    val cluster = spec.toResource()

    // the cluster currently uses v1
    every { resourceToCurrentState(cluster) } returns spec.toActualServerGroups(V1)

    // there are no previous environments to consider
    every { dependentEnvironmentFinder.resourceStatusesInDependentEnvironments(any()) } returns emptyMap()
    every { dependentEnvironmentFinder.resourcesOfSameKindInDependentEnvironments(any<Resource<ClusterSpec>>()) } returns emptyList()

    expectThat(resolver(cluster)).instanceMetadataServiceVersion isEqualTo V2

    verify { featureRolloutRepository.markRolloutStarted(featureName, cluster.id) }
  }

  @Test
  fun `leaves setting alone if it is explicitly specified`() {
    // there are no previous environments to consider
    every { dependentEnvironmentFinder.resourceStatusesInDependentEnvironments(any()) } returns emptyMap()
    every { dependentEnvironmentFinder.resourcesOfSameKindInDependentEnvironments(any<Resource<ClusterSpec>>()) } returns emptyList()

    val cluster = spec.withInstanceMetadataServiceVersion(V1).toResource()

    expectThat(resolver(cluster)).instanceMetadataServiceVersion isEqualTo V1

    verify(exactly = 0) { featureRolloutRepository.markRolloutStarted(any(), any()) }
  }

  @Test
  fun `uses v2 if the cluster is already using v2`() {
    val cluster = spec.toResource()

    // the cluster currently uses v2
    every { resourceToCurrentState(cluster) } returns spec.toActualServerGroups(V2)

    expectThat(resolver(cluster)).instanceMetadataServiceVersion isEqualTo V2

    // this is not considered starting a rollout
    verify(exactly = 0) { featureRolloutRepository.markRolloutStarted(any(), any()) }
  }

  @Test
  fun `does not apply v2 if previous environment is unstable`() {
    val cluster = spec.toResource()

    // the cluster currently uses v2
    every { resourceToCurrentState(cluster) } returns spec.toActualServerGroups(V1)

    // resources in the previous environment are not in a stable state
    every {
      dependentEnvironmentFinder.resourceStatusesInDependentEnvironments(any())
    } returns listOf(previousEnvironmentSpec.toResource()).associate { it.id to Diff }

    expectThat(resolver(cluster)).instanceMetadataServiceVersion isEqualTo V1

    verify(exactly = 0) { featureRolloutRepository.markRolloutStarted(any(), any()) }
  }

  @Test
  fun `does not apply v2 if v2 has not been rolled out to a previous environment`() {
    val cluster = spec.toResource()
    val previousEnvironmentCluster = previousEnvironmentSpec.toResource()

    // the cluster currently uses v1
    every { resourceToCurrentState(cluster) } returns spec.toActualServerGroups(V1)

    // the previous environment is in a stable state…
    every {
      dependentEnvironmentFinder.resourceStatusesInDependentEnvironments((any()))
    } returns listOf(previousEnvironmentCluster).associate { it.id to Ok }

    // … but its clusters are also still using v1
    every {
      dependentEnvironmentFinder.resourcesOfSameKindInDependentEnvironments(any<Resource<ClusterSpec>>())
    } returns listOf(previousEnvironmentCluster)
    every { resourceToCurrentState(previousEnvironmentCluster) } returns previousEnvironmentSpec.toActualServerGroups(V1)

    expectThat(resolver(cluster)).instanceMetadataServiceVersion isEqualTo V1

    verify(exactly = 0) { featureRolloutRepository.markRolloutStarted(any(), any()) }
  }

  @Test
  fun `applies v2 if v2 has successfully been rolled out to a previous environment`() {
    val cluster = spec.toResource()
    val previousEnvironmentCluster = previousEnvironmentSpec.toResource()

    // the cluster currently uses v1
    every { resourceToCurrentState(spec.toResource()) } returns spec.toActualServerGroups(V1)

    // the previous environment is in a stable state…
    every {
      dependentEnvironmentFinder.resourceStatusesInDependentEnvironments((any()))
    } returns listOf(previousEnvironmentCluster).associate { it.id to Ok }
    every {
      dependentEnvironmentFinder.resourcesOfSameKindInDependentEnvironments(any<Resource<ClusterSpec>>())
    } returns listOf(previousEnvironmentCluster)

    // … and its clusters are already upgraded to v2
    every { resourceToCurrentState(previousEnvironmentCluster) } returns previousEnvironmentSpec.toActualServerGroups(V2)

    expectThat(resolver(cluster)).instanceMetadataServiceVersion isEqualTo V2

    verify { featureRolloutRepository.markRolloutStarted(featureName, cluster.id) }
  }

  @Test
  fun `emits an event if v2 rollout has been attempted before and seemingly not worked`() {
    val cluster = spec.toResource()

    // a rollout was attempted before, but the cluster is still using v1 (e.g. failed to start with v2)
    every { featureRolloutRepository.countRolloutAttempts(featureName, cluster.id) } returns 1
    every { resourceToCurrentState(spec.toResource()) } returns spec.toActualServerGroups(V1)

    // there are no previous environments to consider
    every { dependentEnvironmentFinder.resourceStatusesInDependentEnvironments(any()) } returns emptyMap()
    every { dependentEnvironmentFinder.resourcesOfSameKindInDependentEnvironments(any<Resource<ClusterSpec>>()) } returns emptyList()

    // the rollout is attempted again
    expectThat(resolver(cluster)).instanceMetadataServiceVersion isEqualTo V2

    verify { featureRolloutRepository.markRolloutStarted(featureName, cluster.id) }

    // … but we also emit an event to indicate it may not be working
    verify { eventPublisher.publishEvent(FeatureRolloutFailed(featureName, cluster.id)) }
  }

  @Test
  fun `stops rollout if behavior flag is set, v2 rollout has been attempted before and seemingly not worked`() {
    val cluster = spec.toResource()

    // the flag indicating we should stop rollout if it appears to have failed is set
    every {
      springEnvironment.getProperty(
        "keel.rollout.imdsv2.stopOnFailure",
        Boolean::class.java,
        false
      )
    } returns true

    // a rollout was attempted before, but the cluster is still using v1 (e.g. failed to start with v2)
    every { featureRolloutRepository.countRolloutAttempts(featureName, cluster.id) } returns 1
    every { resourceToCurrentState(spec.toResource()) } returns spec.toActualServerGroups(V1)

    // there are no previous environments to consider
    every { dependentEnvironmentFinder.resourceStatusesInDependentEnvironments(any()) } returns emptyMap()
    every { dependentEnvironmentFinder.resourcesOfSameKindInDependentEnvironments(any<Resource<ClusterSpec>>()) } returns emptyList()

    // the rollout is NOT attempted again
    expectThat(resolver(cluster)).instanceMetadataServiceVersion isEqualTo V1
    verify(exactly = 0) { featureRolloutRepository.markRolloutStarted(featureName, cluster.id) }
    verify(exactly = 0) { eventPublisher.publishEvent(ofType<FeatureRolloutAttempted>()) }

    // … but we also emit an event to indicate it may not be working
    verify { eventPublisher.publishEvent(FeatureRolloutFailed(featureName, cluster.id)) }
  }

  private val Assertion.Builder<Resource<ClusterSpec>>.instanceMetadataServiceVersion: Assertion.Builder<InstanceMetadataServiceVersion?>
    get() = get(Resource<ClusterSpec>::spec)
      .get(ClusterSpec::defaults)
      .get(ClusterSpec.ServerGroupSpec::launchConfiguration)
      .isNotNull()
      .get(LaunchConfigurationSpec::instanceMetadataServiceVersion)

  private fun ClusterSpec.withInstanceMetadataServiceVersion(version: InstanceMetadataServiceVersion?) =
    copy(
      _defaults = defaults.copy(
        launchConfiguration = LaunchConfigurationSpec(
          instanceMetadataServiceVersion = version
        )
      )
    )

  private fun ClusterSpec.toResource() =
    resource(
      kind = EC2_CLUSTER_V1_1.kind,
      spec = this
    )

  private fun ClusterSpec.toActualServerGroups(imdsVersion: InstanceMetadataServiceVersion) =
    locations.regions.map { it.name }.associateWith { region ->
      ServerGroup(
        name = "${moniker}-v001",
        location = Location(
          locations.account,
          region,
          locations.vpc!!,
          locations.subnet!!,
          "abc".map { "${region}$it" }.toSet()
        ),
        launchConfiguration = LaunchConfiguration(
          imageId = "ami-001",
          appVersion = "$application-v001",
          baseImageName = "bionic-v001",
          instanceType = "m5.xl",
          iamRole = "${application}Role",
          keyPair = "${application}KeyPair",
          requireIMDSv2 = imdsVersion == V2
        )
      )
    }
}