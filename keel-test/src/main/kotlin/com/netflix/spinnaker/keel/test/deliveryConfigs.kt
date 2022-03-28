package com.netflix.spinnaker.keel.test

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.SimpleLocations
import com.netflix.spinnaker.keel.api.SimpleRegionSpec
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.SubnetAwareRegionSpec
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.ec2.ApplicationLoadBalancerSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterDependencies
import com.netflix.spinnaker.keel.api.ec2.EC2_APPLICATION_LOAD_BALANCER_V1_2
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.api.titus.TitusServerGroupSpec
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.docker.ReferenceProvider

/**
 * Helper functions for working with delivery configs
 */

fun deliveryConfig(
  resource: Resource<*> = resource(),
  env: Environment = Environment("test", setOf(resource)),
  application: String = "fnord",
  configName: String = "myconfig",
  artifact: DeliveryArtifact = DebianArtifact(name = "fnord", deliveryConfigName = configName, vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2"))),
  deliveryConfig: DeliveryConfig = DeliveryConfig(
    name = configName,
    application = application,
    serviceAccount = "keel@spinnaker",
    artifacts = setOf(artifact),
    environments = setOf(env),
    metadata = mapOf("some" to "meta")
  )
): DeliveryConfig {
  return deliveryConfig
}

fun deliveryConfig(
  resources: Set<Resource<*>>,
  env: Environment = Environment("test", resources),
  application: String = "fnord",
  configName: String = "myconfig",
  artifact: DeliveryArtifact = DebianArtifact(name = "fnord", deliveryConfigName = configName, vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2"))),
  deliveryConfig: DeliveryConfig = DeliveryConfig(
    name = configName,
    application = application,
    serviceAccount = "keel@spinnaker",
    artifacts = setOf(artifact),
    environments = setOf(env),
    metadata = mapOf("some" to "meta")
  )
): DeliveryConfig {
  return deliveryConfig
}

/**
 * @return this delivery config updated to replace an existing resource with a newer version.
 */
fun DeliveryConfig.withUpdatedResource(updatedResource: Resource<*>): DeliveryConfig =
  copy(
    environments = environments.mapTo(mutableSetOf()) { environment ->
      if (environment.resources.any { it.id == updatedResource.id }) {
        val newResources = environment.resources.filter { it.id != updatedResource.id } + updatedResource
        environment.copy(resources = newResources.toSet())
      } else {
        environment
      }
    }
  )

fun previewEnvironment(resources: Set<Resource<*>> = setOf(resource())) =
  Environment("test-preview", isPreview = true, resources = resources)
    .addMetadata(mapOf(
      "repoKey" to "stash/proj/repo",
      "branch" to "feature/abc",
      "deliveryConfigName" to "myconfig"
    ))

fun deliveryConfigWithClusterAndLoadBalancer() =
  deliveryConfig(
    artifact = DockerArtifact(
      name = "fnord",
      branch = "main"
    ),
    env = Environment(
      name = "test",
      resources = setOf(
        Resource(
          kind = TITUS_CLUSTER_V1.kind,
          metadata = mapOf("id" to "fnord-test-cluster", "application" to "fnord", "displayName" to "fnord-test-cluster"),
          spec = TitusClusterSpec(
            moniker = Moniker("fnord", "test", "cluster"),
            locations = SimpleLocations(
              account = "test",
              regions = setOf(SimpleRegionSpec("us-east-1"))
            ),
            container = ReferenceProvider("fnord"),
            _defaults = TitusServerGroupSpec(
              dependencies = ClusterDependencies(
                loadBalancerNames = setOf("fnord-test-alb")
              )
            )
          )
        ),
        Resource(
          kind = EC2_APPLICATION_LOAD_BALANCER_V1_2.kind,
          metadata = mapOf("id" to "fnord-test-alb", "application" to "fnord", "displayName" to "fnord-test-alb"),
          spec = ApplicationLoadBalancerSpec(
            moniker = Moniker("fnord", "test", "alb"),
            locations = SubnetAwareLocations(
              account = "test",
              subnet = "internal",
              regions = setOf(SubnetAwareRegionSpec("us-east-1"))
            ),
            listeners = emptySet(),
            targetGroups = emptySet()
          )
        )
      )
    )
  )
