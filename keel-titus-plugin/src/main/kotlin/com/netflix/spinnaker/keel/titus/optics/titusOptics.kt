package com.netflix.spinnaker.keel.titus.optics

import arrow.optics.Lens
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.SimpleLocations
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.CapacitySpec
import com.netflix.spinnaker.keel.api.ec2.ScalingSpec
import com.netflix.spinnaker.keel.api.ec2.TargetTrackingPolicy
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.api.titus.TitusScalingSpec
import com.netflix.spinnaker.keel.api.titus.TitusServerGroupSpec
import com.netflix.spinnaker.keel.optics.monikerStackLens
import com.netflix.spinnaker.keel.optics.simpleLocationsAccountLens

/**
 * Lens for getting/setting [TitusClusterSpec.moniker].
 */
val titusClusterSpecMonikerLens: Lens<TitusClusterSpec, Moniker> = Lens(
  get = TitusClusterSpec::moniker,
  set = { spec, moniker -> spec.copy(moniker = moniker) }
)

/**
 * Composed lens for getting/setting the [Moniker.stack] of a [TitusClusterSpec].
 */
val titusClusterSpecStackLens = titusClusterSpecMonikerLens + monikerStackLens

/**
 * Lens for getting/setting [TitusClusterSpec.locations].
 */
val titusClusterSpecLocationsLens: Lens<TitusClusterSpec, SimpleLocations> = Lens(
  get = TitusClusterSpec::locations,
  set = { spec, locations -> spec.copy(locations = locations) }
)

/**
 * Lens for getting/setting [TitusClusterSpec.defaults].
 */
val titusClusterSpecDefaultsLens: Lens<TitusClusterSpec, TitusServerGroupSpec> = Lens(
  get = TitusClusterSpec::defaults,
  set = { spec, defaults -> spec.copy(_defaults = defaults) }
)

fun titusClusterSpecRegionOverrideLens(region: String): Lens<TitusClusterSpec, TitusServerGroupSpec?> = Lens(
  get = { it.overrides[region] },
  set = { titusClusterSpec, override ->
    titusClusterSpec.run {
      if (override == null) {
        titusClusterSpec.copy(overrides = overrides - region)
      } else {
        titusClusterSpec.copy(overrides = overrides + (region to override))
      }
    }
  }
)

/**
 * Composed lens for getting/setting the [SimpleLocations.account] of a [TitusClusterSpec].
 */
val titusClusterSpecAccountLens =
  titusClusterSpecLocationsLens + simpleLocationsAccountLens

/**
 * Lens for getting/setting [TitusServerGroupSpec.capacity].
 */
val titusServerGroupSpecCapacityLens: Lens<TitusServerGroupSpec, CapacitySpec?> = Lens(
  get = { it.capacity },
  set = { titusServerGroupSpec, capacity -> titusServerGroupSpec.copy(capacity = capacity) }
)

/**
 * Lens for getting/setting [TitusServerGroupSpec.capacity] where the `TitusServerGroupSpec` may be `null`.
 */
val titusServerGroupSpecCapacityLensNullable: Lens<TitusServerGroupSpec?, CapacitySpec?> = Lens(
  get = { it?.capacity },
  set = { titusServerGroupSpec, capacity ->
    titusServerGroupSpec?.copy(capacity = capacity) ?: TitusServerGroupSpec(capacity = capacity)
  }
)

val titusServerGroupSpecScalingLens: Lens<TitusServerGroupSpec, ScalingSpec?> = Lens(
  get = { it.scaling },
  set = { titusServerGroupSpec, scaling ->
    titusServerGroupSpec.copy(scaling = scaling) ?: TitusServerGroupSpec(scaling = scaling)
  }
)

val titusScalingTargetTrackingPoliciesLensNullable: Lens<ScalingSpec?, Set<TargetTrackingPolicy>?> = Lens(
  get = { (it as? TitusScalingSpec)?.targetTrackingPolicies },
  set = { scaling, targetTrackingPolicies ->
    if (targetTrackingPolicies == null) {
      (scaling as? TitusScalingSpec)?.copy(targetTrackingPolicies = emptySet())
    } else {
      (scaling as? TitusScalingSpec)?.copy(targetTrackingPolicies = targetTrackingPolicies) ?: TitusScalingSpec(targetTrackingPolicies = targetTrackingPolicies)
    }
  }
)


/**
 * Lens for getting/setting [TitusServerGroupSpec.containerAttributes].
 */
val titusServerGroupSpecContainerAttributesLens: Lens<TitusServerGroupSpec, Map<String, String>> = Lens(
  get = { it.containerAttributes ?: emptyMap() },
  set = { titusServerGroupSpec, containerAttributes -> titusServerGroupSpec.copy(containerAttributes = containerAttributes) }
)

/**
 * Composed lens for getting/setting the [TitusServerGroupSpec.containerAttributes] of a [TitusClusterSpec.moniker].
 */
val titusClusterSpecContainerAttributesLens =
  titusClusterSpecDefaultsLens + titusServerGroupSpecContainerAttributesLens
