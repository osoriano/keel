package com.netflix.spinnaker.keel.api.ec2.old

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id.DEDUCTION
import com.fasterxml.jackson.annotation.JsonUnwrapped
import com.netflix.spinnaker.keel.api.ArtifactReferenceProvider
import com.netflix.spinnaker.keel.api.ClusterDeployStrategy
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.RedBlack
import com.netflix.spinnaker.keel.api.SpinnakerResourceSpec
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.ec2.ClusterDependencies
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.CapacitySpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.HealthSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.ServerGroupSpec
import com.netflix.spinnaker.keel.api.ec2.EC2ScalingSpec
import com.netflix.spinnaker.keel.api.ec2.LaunchConfigurationSpec
import com.netflix.spinnaker.keel.api.ec2.OverrideableClusterDependencyContainer
import com.netflix.spinnaker.keel.api.ec2.ScalingSpec
import com.netflix.spinnaker.keel.api.schema.Factory
import com.netflix.spinnaker.keel.api.schema.Optional

data class ClusterV1Spec (
  override val moniker: Moniker,
  val imageProvider: ImageProvider? = null,
  val deployWith: ClusterDeployStrategy = RedBlack(),
  override val locations: SubnetAwareLocations,
  private val _defaults: ServerGroupSpec,
  @get:JsonInclude(NON_EMPTY)
  override val overrides: Map<String, ServerGroupSpec> = emptyMap()
) : SpinnakerResourceSpec<SubnetAwareLocations>, OverrideableClusterDependencyContainer<ServerGroupSpec>, ArtifactReferenceProvider {
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
    @JsonTypeInfo(use = DEDUCTION, defaultImpl = EC2ScalingSpec::class)
    scaling: ScalingSpec? = null,
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

  companion object {
    const val MAX_NAME_LENGTH = 255
  }

  @get:JsonUnwrapped
  override val defaults: ServerGroupSpec
    get() = _defaults

  @get:JsonIgnore
  override val artifactReference: String?
    get() = imageProvider?.reference

  override fun withArtifactReference(reference: String) =
    copy(imageProvider = ImageProvider(reference = reference))

  override fun deepRename(suffix: String) =
    copy(moniker = moniker.withSuffix(suffix,canTruncateStack = false, maxNameLength = MAX_NAME_LENGTH))

  data class ImageProvider(
    val reference: String
  )
}
