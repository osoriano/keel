package com.netflix.spinnaker.keel.api.ec2

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id.DEDUCTION
import com.fasterxml.jackson.annotation.JsonUnwrapped
import com.netflix.spinnaker.keel.api.ClusterDeployStrategy
import com.netflix.spinnaker.keel.api.ComputeResourceSpec
import com.netflix.spinnaker.keel.api.Dependency
import com.netflix.spinnaker.keel.api.DependencyType.LOAD_BALANCER
import com.netflix.spinnaker.keel.api.DependencyType.SECURITY_GROUP
import com.netflix.spinnaker.keel.api.DependencyType.TARGET_GROUP
import com.netflix.spinnaker.keel.api.Dependent
import com.netflix.spinnaker.keel.api.ExcludedFromDiff
import com.netflix.spinnaker.keel.api.Locations
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.RedBlack
import com.netflix.spinnaker.keel.api.RolloutConfig
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.SubnetAwareRegionSpec
import com.netflix.spinnaker.keel.api.artifacts.ArtifactType
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.ServerGroupSpec
import com.netflix.spinnaker.keel.api.ec2.ServerGroup.Health
import com.netflix.spinnaker.keel.api.ec2.ServerGroup.LaunchConfiguration
import com.netflix.spinnaker.keel.api.schema.Description
import com.netflix.spinnaker.keel.api.schema.Factory
import com.netflix.spinnaker.keel.api.schema.Optional
import com.netflix.spinnaker.keel.api.schema.Title
import java.time.Duration

/**
 * Transforms a [ClusterSpec] into a concrete model of server group desired states.
 */
fun ClusterSpec.resolve(): Set<ServerGroup> =
  locations.regions.map {
    ServerGroup(
      name = moniker.toString(),
      location = Location(
        account = locations.account,
        region = it.name,
        vpc = locations.vpc ?: error("No vpc supplied or resolved"),
        subnet = locations.subnet ?: error("No subnet purpose supplied or resolved"),
        availabilityZones = it.availabilityZones
      ),
      launchConfiguration = resolveLaunchConfiguration(it),
      capacity = resolveCapacity(it.name),
      dependencies = resolveDependencies(it.name),
      health = resolveHealth(it.name),
      scaling = resolveScaling(it.name),
      tags = resolveTags(it.name),
      artifactName = artifactName,
      artifactVersion = artifactVersion
    )
  }
    .toSet()

fun ClusterSpec.resolveTags(region: String? = null) =
  defaults.tags + (region?.let { overrides[it] }?.tags ?: emptyMap())

private fun ClusterSpec.resolveLaunchConfiguration(region: SubnetAwareRegionSpec): LaunchConfiguration {
  val image = checkNotNull(
    overrides[region.name]?.launchConfiguration?.image
      ?: defaults.launchConfiguration?.image
  ) { "No image resolved / specified for ${region.name}" }
  return LaunchConfiguration(
    appVersion = image.appVersion,
    baseImageName = image.baseImageName,
    imageId = image.id,
    instanceType = checkNotNull(
      overrides[region.name]?.launchConfiguration?.instanceType
        ?: defaults.launchConfiguration?.instanceType
    ) {
      "No instance type resolved for $moniker (region ${region.name}) and cannot determine a default"
    },
    ebsOptimized = checkNotNull(
      overrides[region.name]?.launchConfiguration?.ebsOptimized
        ?: defaults.launchConfiguration?.ebsOptimized
        ?: LaunchConfiguration.DEFAULT_EBS_OPTIMIZED
    ),
    iamRole = checkNotNull(
      overrides[region.name]?.launchConfiguration?.iamRole
        ?: defaults.launchConfiguration?.iamRole
        ?: LaunchConfiguration.defaultIamRoleFor(moniker.app)
    ),
    keyPair = checkNotNull(
      overrides[region.name]?.launchConfiguration?.keyPair
        ?: defaults.launchConfiguration?.keyPair
    ) {
      "No keypair resolved for $moniker (region ${region.name}) and cannot determine a default"
    },
    instanceMonitoring = overrides[region.name]?.launchConfiguration?.instanceMonitoring
      ?: defaults.launchConfiguration?.instanceMonitoring
      ?: LaunchConfiguration.DEFAULT_INSTANCE_MONITORING,
    ramdiskId = overrides[region.name]?.launchConfiguration?.ramdiskId
      ?: defaults.launchConfiguration?.ramdiskId,
    requireIMDSv2 = (overrides[region.name]?.launchConfiguration?.instanceMetadataServiceVersion
      ?: defaults.launchConfiguration?.instanceMetadataServiceVersion) == InstanceMetadataServiceVersion.V2,
    associateIPv6Address = overrides[region.name]?.launchConfiguration?.associateIPv6Address
      ?: defaults.launchConfiguration?.associateIPv6Address
      ?: false
  )
}

fun ClusterSpec.resolveCapacity(region: String? = null): Capacity {
  val regionOverrides = overrides[region]
  val hasScalingPolicies = (regionOverrides?.scaling ?: defaults.scaling).hasScalingPolicies()
  return when (region) {
    null -> defaults.resolveCapacity(hasScalingPolicies) ?: Capacity.DefaultCapacity(1, 1, 1)
    else -> regionOverrides?.resolveCapacity(hasScalingPolicies) ?: defaults.resolveCapacity(hasScalingPolicies) ?: Capacity.DefaultCapacity(1, 1, 1)
  }
}

fun ServerGroupSpec.resolveCapacity(hasScalingPolicies: Boolean): Capacity? =
  when {
    capacity == null -> null
    hasScalingPolicies -> Capacity.AutoScalingCapacity(capacity)
    else -> Capacity.DefaultCapacity(capacity)
  }

fun ClusterSpec.resolveScaling(region: String? = null): Scaling {
  val defaultScaling = defaults.scaling as? EC2ScalingSpec
  val regionalScaling = overrides[region]?.scaling as? EC2ScalingSpec
  return Scaling(
    suspendedProcesses = defaultScaling?.suspendedProcesses +
      (regionalScaling?.suspendedProcesses ?: emptySet()),
    targetTrackingPolicies = defaultScaling?.targetTrackingPolicies +
      (regionalScaling?.targetTrackingPolicies ?: emptySet()),
    stepScalingPolicies = defaultScaling?.stepScalingPolicies +
      (regionalScaling?.stepScalingPolicies ?: emptySet())
  )
}

fun ClusterSpec.resolveDependencies(region: String? = null): ClusterDependencies =
  ClusterDependencies(
    loadBalancerNames = defaults.dependencies?.loadBalancerNames +
      (region?.let { overrides[it] }?.dependencies?.loadBalancerNames ?: emptySet()),
    securityGroupNames = defaults.dependencies?.securityGroupNames +
      (region?.let { overrides[it] }?.dependencies?.securityGroupNames ?: emptySet()),
    targetGroups = defaults.dependencies?.targetGroups +
      (region?.let { overrides[it] }?.dependencies?.targetGroups ?: emptySet())
  )

fun ClusterSpec.resolveHealth(region: String? = null): Health {
  val default by lazy { Health() }
  return Health(
    cooldown = region?.let { overrides[it] }?.health?.cooldown
      ?: defaults.health?.cooldown
      ?: default.cooldown,
    warmup = region?.let { overrides[it] }?.health?.warmup
      ?: defaults.health?.warmup
      ?: default.warmup,
    healthCheckType = region?.let { overrides[it] }?.health?.healthCheckType
      ?: defaults.health?.healthCheckType
      ?: default.healthCheckType,
    enabledMetrics = region?.let { overrides[it] }?.health?.enabledMetrics
      ?: defaults.health?.enabledMetrics
      ?: default.enabledMetrics,
    terminationPolicies = region?.let { overrides[it] }?.health?.terminationPolicies
      ?: defaults.health?.terminationPolicies
      ?: default.terminationPolicies
  )
}

@Title("Cluster")
data class ClusterSpec(
  override val moniker: Moniker,
  override val artifactReference: String? = null,
  val deployWith: ClusterDeployStrategy = RedBlack(),
  val rolloutWith: RolloutConfig? = null,
  override val locations: SubnetAwareLocations,
  private val _defaults: ServerGroupSpec,
  @get:JsonInclude(NON_EMPTY)
  override val overrides: Map<String, ServerGroupSpec> = emptyMap(),
  override val artifactName: String? = null,
  override val artifactVersion: String? = null
) : ComputeResourceSpec<SubnetAwareLocations>, OverrideableClusterDependencyContainer<ServerGroupSpec>, Dependent {

  companion object {
    const val MAX_NAME_LENGTH = 255
  }

  @Factory
  constructor(
    moniker: Moniker,
    artifactReference: String? = null,
    deployWith: ClusterDeployStrategy = RedBlack(),
    @Optional locations: SubnetAwareLocations,
    launchConfiguration: LaunchConfigurationSpec? = null,
    capacity: CapacitySpec? = null,
    dependencies: ClusterDependencies? = null,
    health: HealthSpec? = null,
    @JsonTypeInfo(use = DEDUCTION, defaultImpl = EC2ScalingSpec::class)
    scaling: ScalingSpec? = null,
    tags: Map<String, String>? = null,
    overrides: Map<String, ServerGroupSpec> = emptyMap(),
    rolloutWith: RolloutConfig? = null
  ) : this(
    moniker,
    artifactReference,
    deployWith,
    rolloutWith,
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

  /**
   * I have no idea why, but if I annotate the constructor property with @get:JsonUnwrapped, the
   * @JsonCreator constructor below nulls out everything in the ClusterServerGroupSpec some time
   * very late in parsing. Using a debugger I can see it assigning the object correctly but then it
   * seems to overwrite it. This is a bit nasty but I think having the cluster-wide defaults at the
   * top level in the cluster spec YAML / JSON is nicer for the user.
   */
  @get:JsonUnwrapped
  override val defaults: ServerGroupSpec
    get() = _defaults

  override val artifactType: ArtifactType = DEBIAN

  @get:ExcludedFromDiff
  override val dependsOn: Set<Dependency>
    get() = locations.regions.flatMap { region ->
      val deps = mutableListOf<Dependency>()
      _defaults.dependencies?.loadBalancerNames?.forEach {
        deps.add(Dependency(LOAD_BALANCER, region.name, it))
      }
      _defaults.dependencies?.securityGroupNames?.forEach {
        deps.add(Dependency(SECURITY_GROUP, region.name, it))
      }
      _defaults.dependencies?.targetGroups?.forEach {
        deps.add(Dependency(TARGET_GROUP, region.name, it))
      }
      overrides[region.name]?.dependencies?.loadBalancerNames?.forEach {
        deps.add(Dependency(LOAD_BALANCER, region.name, it))
      }
      overrides[region.name]?.dependencies?.securityGroupNames?.forEach {
        deps.add(Dependency(SECURITY_GROUP, region.name, it))
      }
      overrides[region.name]?.dependencies?.targetGroups?.forEach {
        deps.add(Dependency(TARGET_GROUP, region.name, it))
      }
      deps
    }.toSet()

  override fun deepRename(suffix: String) =
    copy(moniker = moniker.withSuffix(suffix, canTruncateStack = false, maxNameLength = MAX_NAME_LENGTH))

  override fun withArtifactReference(reference: String) =
    copy(artifactReference = reference)

  data class ServerGroupSpec(
    val launchConfiguration: LaunchConfigurationSpec? = null,
    val capacity: CapacitySpec? = null,
    override val dependencies: ClusterDependencies? = null,
    val health: HealthSpec? = null,
    @JsonTypeInfo(use = DEDUCTION, defaultImpl = EC2ScalingSpec::class)
    val scaling: ScalingSpec? = null,
    @get:JsonInclude(NON_EMPTY)
    val tags: Map<String, String>? = null
  ) : ClusterDependencyContainer {
    init {
      // Require capacity.desired or scaling policies, or let them both be blank for constructing overrides
      require(!(capacity?.desired != null && scaling.hasScalingPolicies())) {
        "capacity.desired and auto-scaling policies are mutually exclusive: current = $capacity, $scaling"
      }
    }
  }

  /**
   * Capacity definition with an optional [desired] which _must_ be `null` if the server group has scaling policies.
   */
  @Title("Capacity")
  data class CapacitySpec(
    val min: Int,
    val max: Int,
    @Description("The desired number of instances. Must be omitted or null if the server group has scaling policies")
    val desired: Int? = null
  )

  @Title("Health")
  @JsonInclude(NON_EMPTY)
  data class HealthSpec(
    val cooldown: Duration? = null,
    val warmup: Duration? = null,
    val healthCheckType: HealthCheckType? = null,
    val enabledMetrics: Set<Metric>? = null,
    val terminationPolicies: Set<TerminationPolicy>? = null
  )
}

interface ScalingSpec

@Title("EC2 Scaling")
data class EC2ScalingSpec(
  val suspendedProcesses: Set<ScalingProcess> = emptySet(),
  val targetTrackingPolicies: Set<TargetTrackingPolicy> = emptySet(),
  val stepScalingPolicies: Set<StepScalingPolicy> = emptySet()
) : ScalingSpec

operator fun Locations<SubnetAwareRegionSpec>.get(region: String) =
  regions.first { it.name == region }

private operator fun <E> Set<E>?.plus(elements: Set<E>?): Set<E> =
  when {
    this == null || isEmpty() -> elements ?: emptySet()
    elements == null || elements.isEmpty() -> this
    else -> mutableSetOf<E>().also {
      it.addAll(this)
      it.addAll(elements)
    }
  }

private operator fun <K, V> Map<K, V>?.plus(map: Map<K, V>?): Map<K, V> =
  when {
    this == null || isEmpty() -> map ?: emptyMap()
    map == null || map.isEmpty() -> this
    else -> mutableMapOf<K, V>().also {
      it.putAll(this)
      it.putAll(map)
    }
  }
