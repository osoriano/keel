package com.netflix.spinnaker.keel.api.ec2.old

import com.netflix.spinnaker.keel.api.ArtifactReferenceProvider
import com.netflix.spinnaker.keel.api.ClusterDeployStrategy
import com.netflix.spinnaker.keel.api.ComputeResourceSpec
import com.netflix.spinnaker.keel.api.Locatable
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.Monikered
import com.netflix.spinnaker.keel.api.RedBlack
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.artifacts.ArtifactType
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.ec2.ClusterDependencies
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.CapacitySpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.HealthSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.ServerGroupSpec
import com.netflix.spinnaker.keel.api.ec2.LaunchConfigurationSpec
import com.netflix.spinnaker.keel.api.ec2.OverrideableClusterDependencyContainer
import com.netflix.spinnaker.keel.api.ec2.Scaling
import com.netflix.spinnaker.keel.api.schema.Factory
import com.netflix.spinnaker.keel.api.schema.Optional

data class ClusterV1Spec (
  override val moniker: Moniker,
  val imageProvider: ImageProvider? = null,
  val deployWith: ClusterDeployStrategy = RedBlack(),
  override val locations: SubnetAwareLocations,
  private val _defaults: ServerGroupSpec,
  override val overrides: Map<String, ServerGroupSpec> = emptyMap()
) : Monikered, Locatable<SubnetAwareLocations>, OverrideableClusterDependencyContainer<ServerGroupSpec>, ArtifactReferenceProvider {
  @Factory
  constructor(
    moniker: Moniker,
    imageProvider: ImageProvider? = null,
    deployWith: ClusterDeployStrategy = RedBlack(),
    @Optional locations: SubnetAwareLocations,
    launchConfiguration: LaunchConfigurationSpec? = null,
    capacity: CapacitySpec? = null,
    dependencies: ClusterDependencies? = null,
    health: HealthSpec? = null,
    scaling: Scaling? = null,
    tags: Map<String, String>? = null,
    overrides: Map<String, ServerGroupSpec> = emptyMap()
  ) : this(
    moniker,
    imageProvider,
    deployWith,
    locations,
    ServerGroupSpec(
      launchConfiguration,
      capacity,
      dependencies,
      health,
      scaling,
      tags
    ),
    overrides
  )

  override val id = "${locations.account}:$moniker"

  override val defaults: ServerGroupSpec
    get() = _defaults

  override val artifactReference: String?
    get() = imageProvider?.reference

  override fun withArtifactReference(reference: String) =
    copy(imageProvider = ImageProvider(reference = reference))

  override fun deepRename(suffix: String) =
    copy(moniker = moniker.withSuffix(suffix))

  data class ImageProvider(
    val reference: String
  )
}
