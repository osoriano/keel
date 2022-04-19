package com.netflix.spinnaker.keel.ec2.optics

import arrow.optics.Lens
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.CapacitySpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.ServerGroupSpec
import com.netflix.spinnaker.keel.api.ec2.EC2ScalingSpec
import com.netflix.spinnaker.keel.api.ec2.InstanceMetadataServiceVersion
import com.netflix.spinnaker.keel.api.ec2.LaunchConfigurationSpec
import com.netflix.spinnaker.keel.api.ec2.ScalingProcess
import com.netflix.spinnaker.keel.api.ec2.ScalingSpec
import com.netflix.spinnaker.keel.api.ec2.TargetTrackingPolicy
import com.netflix.spinnaker.keel.optics.monikerStackLens
import com.netflix.spinnaker.keel.optics.subnetAwareLocationsAccountLens

/**
 * Lens for getting/setting [ClusterSpec.moniker].
 */
val clusterSpecMonikerLens: Lens<ClusterSpec, Moniker> = Lens(
  get = ClusterSpec::moniker,
  set = { spec, moniker -> spec.copy(moniker = moniker) }
)

/**
 * Composed lens for getting/setting the [Moniker.stack] of a [ClusterSpec].
 */
val clusterSpecStackLens = clusterSpecMonikerLens + monikerStackLens

/**
 * Lens for getting/setting [ClusterSpec.locations].
 */
val clusterSpecLocationsLens: Lens<ClusterSpec, SubnetAwareLocations> = Lens(
  get = ClusterSpec::locations,
  set = { spec, locations -> spec.copy(locations = locations) }
)

/**
 * Composed lens for getting/setting the [SubnetAwareLocations.account] of a [ClusterSpec].
 */
val clusterSpecAccountLens =
  clusterSpecLocationsLens + subnetAwareLocationsAccountLens

val clusterSpecDefaultsLens: Lens<ClusterSpec, ServerGroupSpec> = Lens(
  get = ClusterSpec::defaults,
  set = { spec, defaults -> spec.copy(_defaults = defaults) }
)

val serverGroupSpecLaunchConfigurationSpecLens: Lens<ServerGroupSpec, LaunchConfigurationSpec?> = Lens(
  get = ServerGroupSpec::launchConfiguration,
  set = { serverGroupSpec, launchConfigurationSpec -> serverGroupSpec.copy(launchConfiguration = launchConfigurationSpec) }
)

val launchConfigurationSpecInstanceMetadataServiceVersionLens: Lens<LaunchConfigurationSpec?, InstanceMetadataServiceVersion?> =
  Lens(
    get = { it?.instanceMetadataServiceVersion },
    set = { launchConfigurationSpec, instanceMetadataServiceVersion ->
      launchConfigurationSpec?.copy(
        instanceMetadataServiceVersion = instanceMetadataServiceVersion
      ) ?: LaunchConfigurationSpec(
        instanceMetadataServiceVersion = instanceMetadataServiceVersion
      )
    }
  )

val serverGroupSpecCapacityLens: Lens<ServerGroupSpec, CapacitySpec?> = Lens(
  get = ServerGroupSpec::capacity,
  set = { serverGroupSpec, capacity -> serverGroupSpec.copy(capacity = capacity) }
)

val serverGroupSpecScalingLens: Lens<ServerGroupSpec, ScalingSpec?> = Lens(
  get = ServerGroupSpec::scaling,
  set = { serverGroupSpec, scaling -> serverGroupSpec.copy(scaling = scaling) }
)

val serverGroupSpecScalingLensNullable: Lens<ServerGroupSpec?, ScalingSpec?> = Lens(
  get = { it?.scaling },
  set = { serverGroupSpec, scaling ->
    if (scaling == null) {
      serverGroupSpec?.copy(scaling = null)
    } else {
      serverGroupSpec?.copy(scaling = scaling) ?: ServerGroupSpec(scaling = scaling)
    }
  }
)

val scalingSuspendedProcessesLensNullable: Lens<ScalingSpec?, Set<ScalingProcess>?> = Lens(
  get = { (it as? EC2ScalingSpec)?.suspendedProcesses },
  set = { scaling, suspendedProcesses ->
    if (suspendedProcesses == null) {
      (scaling as? EC2ScalingSpec)?.copy(suspendedProcesses = emptySet())
    } else {
      (scaling as? EC2ScalingSpec)?.copy(suspendedProcesses = suspendedProcesses) ?: EC2ScalingSpec(suspendedProcesses = suspendedProcesses)
    }
  }
)

val scalingTargetTrackingPoliciesLensNullable: Lens<ScalingSpec?, Set<TargetTrackingPolicy>?> = Lens(
  get = { (it as? EC2ScalingSpec)?.targetTrackingPolicies },
  set = { scaling, targetTrackingPolicies ->
    if (targetTrackingPolicies == null) {
      (scaling as? EC2ScalingSpec)?.copy(targetTrackingPolicies = emptySet())
    } else {
      (scaling as? EC2ScalingSpec)?.copy(targetTrackingPolicies = targetTrackingPolicies) ?: EC2ScalingSpec(targetTrackingPolicies = targetTrackingPolicies)
    }
  }
)

val serverGroupSpecCapacityLensNullable: Lens<ServerGroupSpec?, CapacitySpec?> = Lens(
  get = { it?.capacity },
  set = { serverGroupSpec, capacity ->
    if (capacity == null) {
      serverGroupSpec?.copy(capacity = null)
    } else {
      serverGroupSpec?.copy(capacity = capacity) ?: ServerGroupSpec(capacity = capacity)
    }
  }
)

fun clusterSpecRegionOverrideLens(region: String): Lens<ClusterSpec, ServerGroupSpec?> = Lens(
  get = { it.overrides[region] },
  set = { clusterSpec, override ->
    clusterSpec.run {
      if (override == null) {
        copy(overrides = overrides - region)
      } else {
        copy(overrides = overrides + (region to override))
      }
    }
  }
)

val launchConfigurationSpecAssociateIPv6AddressLens: Lens<LaunchConfigurationSpec?, Boolean?> = Lens(
  get = { it?.associateIPv6Address },
  set = { spec, associateIPv6Address ->
    if (associateIPv6Address == null) {
      spec?.copy(associateIPv6Address = null)
    } else {
      spec?.copy(associateIPv6Address = associateIPv6Address)
        ?: LaunchConfigurationSpec(associateIPv6Address = associateIPv6Address)
    }
  }
)

val clusterSpecAssociateIPv6AddressLens: Lens<ClusterSpec, Boolean?> =
  clusterSpecDefaultsLens + serverGroupSpecLaunchConfigurationSpecLens + launchConfigurationSpecAssociateIPv6AddressLens
