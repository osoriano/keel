package com.netflix.spinnaker.keel.ec2.optics

import arrow.optics.Lens
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.CapacitySpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.ServerGroupSpec
import com.netflix.spinnaker.keel.api.ec2.InstanceMetadataServiceVersion
import com.netflix.spinnaker.keel.api.ec2.LaunchConfigurationSpec
import com.netflix.spinnaker.keel.api.ec2.Scaling
import com.netflix.spinnaker.keel.api.ec2.ScalingProcess
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

val serverGroupSpecScalingLens: Lens<ServerGroupSpec, Scaling?> = Lens(
  get = ServerGroupSpec::scaling,
  set = { serverGroupSpec, scaling -> serverGroupSpec.copy(scaling = scaling) }
)

val serverGroupSpecScalingLensNullable: Lens<ServerGroupSpec?, Scaling?> = Lens(
  get = { it?.scaling },
  set = { serverGroupSpec, scaling ->
    if (scaling == null) {
      serverGroupSpec?.copy(scaling = null)
    } else {
      serverGroupSpec?.copy(scaling = scaling) ?: ServerGroupSpec(scaling = scaling)
    }
  }
)

val scalingSuspendedProcessesLensNullable: Lens<Scaling?, Set<ScalingProcess>?> = Lens(
  get = { it?.suspendedProcesses },
  set = { scaling, suspendedProcesses ->
    if (suspendedProcesses == null) {
      scaling?.copy(suspendedProcesses = emptySet())
    } else {
      scaling?.copy(suspendedProcesses = suspendedProcesses) ?: Scaling(suspendedProcesses = suspendedProcesses)
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
