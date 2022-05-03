package com.netflix.spinnaker.keel.titus

import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.netflix.buoy.sdk.model.RolloutTarget
import com.netflix.spinnaker.keel.api.ArtifactBridge
import com.netflix.spinnaker.keel.api.ClusterDeployStrategy
import com.netflix.spinnaker.keel.api.Exportable
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.NoStrategy
import com.netflix.spinnaker.keel.api.RedBlack
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.ResourceDiff
import com.netflix.spinnaker.keel.api.ResourceDiffFactory
import com.netflix.spinnaker.keel.api.SimpleLocations
import com.netflix.spinnaker.keel.api.SimpleRegionSpec
import com.netflix.spinnaker.keel.api.actuation.Job
import com.netflix.spinnaker.keel.api.actuation.TaskLauncher
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.BRANCH_JOB_COMMIT_BY_JOB
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.INCREASING_TAG
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.SEMVER_JOB_COMMIT_BY_SEMVER
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.SEMVER_TAG
import com.netflix.spinnaker.keel.api.ec2.Capacity
import com.netflix.spinnaker.keel.api.ec2.Capacity.AutoScalingCapacity
import com.netflix.spinnaker.keel.api.ec2.Capacity.DefaultCapacity
import com.netflix.spinnaker.keel.api.ec2.ClusterDependencies
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.CapacitySpec
import com.netflix.spinnaker.keel.api.ec2.CustomizedMetricSpecification
import com.netflix.spinnaker.keel.api.ec2.MetricDimension
import com.netflix.spinnaker.keel.api.ec2.Scaling
import com.netflix.spinnaker.keel.api.ec2.ServerGroup.InstanceCounts
import com.netflix.spinnaker.keel.api.ec2.StepAdjustment
import com.netflix.spinnaker.keel.api.ec2.StepScalingPolicy
import com.netflix.spinnaker.keel.api.ec2.TargetTrackingPolicy
import com.netflix.spinnaker.keel.api.ec2.hasScalingPolicies
import com.netflix.spinnaker.keel.api.ec2.intersectingConfigurations
import com.netflix.spinnaker.keel.api.ec2.notPresentOrDuplicatedIn
import com.netflix.spinnaker.keel.api.plugins.BaseClusterHandler
import com.netflix.spinnaker.keel.api.plugins.CurrentImages
import com.netflix.spinnaker.keel.api.plugins.ImageInRegion
import com.netflix.spinnaker.keel.api.plugins.Resolver
import com.netflix.spinnaker.keel.api.plugins.getOrder
import com.netflix.spinnaker.keel.api.plugins.getPostDeployWait
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.api.titus.ElasticFileSystem
import com.netflix.spinnaker.keel.api.titus.ResourcesSpec
import com.netflix.spinnaker.keel.api.titus.TITUS_CLOUD_PROVIDER
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.api.titus.TitusScalingSpec
import com.netflix.spinnaker.keel.api.titus.TitusServerGroup
import com.netflix.spinnaker.keel.api.titus.TitusServerGroup.Constraints
import com.netflix.spinnaker.keel.api.titus.TitusServerGroup.Location
import com.netflix.spinnaker.keel.api.titus.TitusServerGroup.MigrationPolicy
import com.netflix.spinnaker.keel.api.titus.TitusServerGroup.NetworkMode
import com.netflix.spinnaker.keel.api.titus.TitusServerGroup.Resources
import com.netflix.spinnaker.keel.api.titus.TitusServerGroupSpec
import com.netflix.spinnaker.keel.api.withDefaultsOmitted
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.clouddriver.ResourceNotFound
import com.netflix.spinnaker.keel.clouddriver.model.CustomizedMetricSpecificationModel
import com.netflix.spinnaker.keel.clouddriver.model.MetricDimensionModel
import com.netflix.spinnaker.keel.clouddriver.model.PredefinedMetricSpecificationModel
import com.netflix.spinnaker.keel.clouddriver.model.ServerGroup
import com.netflix.spinnaker.keel.clouddriver.model.StepAdjustmentModel
import com.netflix.spinnaker.keel.clouddriver.model.TitusActiveServerGroup
import com.netflix.spinnaker.keel.clouddriver.model.TitusScaling.Policy.StepPolicy
import com.netflix.spinnaker.keel.clouddriver.model.TitusScaling.Policy.TargetPolicy
import com.netflix.spinnaker.keel.clouddriver.model.toActive
import com.netflix.spinnaker.keel.core.api.DEFAULT_SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.core.orcaClusterMoniker
import com.netflix.spinnaker.keel.core.serverGroup
import com.netflix.spinnaker.keel.docker.DigestProvider
import com.netflix.spinnaker.keel.docker.ReferenceProvider
import com.netflix.spinnaker.keel.events.ResourceHealthEvent
import com.netflix.spinnaker.keel.exceptions.ActiveServerGroupsException
import com.netflix.spinnaker.keel.exceptions.ArtifactNotSupportedException
import com.netflix.spinnaker.keel.exceptions.DockerArtifactExportError
import com.netflix.spinnaker.keel.exceptions.ExportError
import com.netflix.spinnaker.keel.orca.ClusterExportHelper
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.orca.toOrcaJobProperties
import com.netflix.spinnaker.keel.plugin.buildSpecFromDiff
import com.netflix.spinnaker.keel.retrofit.isNotFound
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.titus.registry.TitusRegistryService
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import net.swiftzer.semver.SemVer
import retrofit2.HttpException
import java.time.Clock
import java.time.Duration
import java.time.Duration.ZERO
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import com.netflix.buoy.sdk.model.Location as RolloutLocation
import com.netflix.spinnaker.keel.clouddriver.model.TitusServerGroup as ClouddriverTitusServerGroup

/**
 * [ResourceHandler] implementation for Titus clusters, represented by [TitusClusterSpec].
 */
class TitusClusterHandler(
  private val cloudDriverService: CloudDriverService,
  private val cloudDriverCache: CloudDriverCache,
  private val orcaService: OrcaService,
  override val clock: Clock,
  taskLauncher: TaskLauncher,
  override val eventPublisher: EventPublisher,
  resolvers: List<Resolver<*>>,
  private val clusterExportHelper: ClusterExportHelper,
  diffFactory: ResourceDiffFactory,
  private val titusRegistryService: TitusRegistryService,
  private val artifactBridge: ArtifactBridge
) : BaseClusterHandler<TitusClusterSpec, TitusServerGroup>(resolvers, taskLauncher, diffFactory) {

  private val mapper = configuredObjectMapper()

  override val supportedKind = TITUS_CLUSTER_V1

  override val cloudProvider = "titus"

  override suspend fun toResolvedType(resource: Resource<TitusClusterSpec>): Map<String, TitusServerGroup> =
    with(resource.spec) {
      resolve().byRegion()
    }

  override suspend fun current(resource: Resource<TitusClusterSpec>): Map<String, TitusServerGroup> {
    val activeServerGroups = cloudDriverService.getActiveServerGroups(resource)
    val allHaveSameImage = activeServerGroups.distinctBy { it.container.digest }.size == 1

    // collect info about unhealthy regions
    val unhealthyRegions = mutableListOf<String>()
    activeServerGroups.forEach { serverGroup ->
      if (serverGroup.instanceCounts?.isHealthy(resource.spec.deployWith.health, serverGroup.capacity) == false) {
        unhealthyRegions.add(serverGroup.location.region)
      }
    }

    // publish health event
    val allHealthy = unhealthyRegions.isEmpty()
    eventPublisher.publishEvent(
      ResourceHealthEvent(
        resource,
        allHealthy,
        unhealthyRegions,
        resource.spec.locations.regions.size
      )
    )

    log.debug("Checking if artifact should be marked as deployed in Titus cluster ${resource.id}: " +
      "same container in all ASGs = $allHaveSameImage, unhealthy regions = $unhealthyRegions")

    if (allHaveSameImage && allHealthy) {
      // only publish a successfully deployed event if the server group is healthy
      val container = activeServerGroups.first().container
      getTagsForDigest(container, resource.spec.locations.account)
        .apply {
          log.debug("Marking ${this.size} tags for container $container as deployed to Titus cluster ${resource.id}: $this")
        }
        .forEach { tag ->
          // We publish an event for each tag that matches the digest
          // so that we handle the tags like `latest` where more than one tags have the same digest
          // and we don't care about some of them.
          notifyArtifactDeployed(resource, tag)
        }
    }

    return activeServerGroups.byRegion()
  }

  override suspend fun actuationInProgress(resource: Resource<TitusClusterSpec>): Boolean =
    generateCorrelationIds(resource).any { correlationId ->
      orcaService
        .getCorrelatedExecutions(correlationId)
        .isNotEmpty()
    }

  override fun getDesiredRegion(diff: ResourceDiff<TitusServerGroup>): String =
    diff.desired.location.region

  override fun getUnhealthyRegionsForActiveServerGroup(resource: Resource<TitusClusterSpec>): List<String> {
    val unhealthyRegions = mutableListOf<String>()
    val activeServerGroups = runBlocking {
      cloudDriverService.getActiveServerGroups(resource)
    }

    activeServerGroups.forEach { serverGroup ->
      val healthy = isHealthy(serverGroup, resource)
      if (!healthy) {
        unhealthyRegions.add(serverGroup.location.region)
      }
    }

    return unhealthyRegions
  }

  override fun isHealthy(serverGroup: TitusServerGroup, resource: Resource<TitusClusterSpec>): Boolean =
    serverGroup.instanceCounts?.isHealthy(
      resource.spec.deployWith.health,
      resource.spec.resolveCapacity(serverGroup.location.region)
    ) == true

  override suspend fun getImage(resource: Resource<TitusClusterSpec>): CurrentImages =
    current(resource)
      .map { (region, serverGroup) ->
        ImageInRegion(region, serverGroup.container.digest, serverGroup.location.account)
      }
      .let { images ->
        CurrentImages(supportedKind.kind, images, resource.id)
      }


  override fun getDesiredAccount(diff: ResourceDiff<TitusServerGroup>): String =
    diff.desired.location.account

  override fun ResourceDiff<TitusServerGroup>.moniker(): Moniker =
    desired.moniker

  override fun TitusServerGroup.moniker(): Moniker =
    moniker

  override fun TitusServerGroup.serverGroup(): String =
    name

  override fun ResourceDiff<TitusServerGroup>.version(resource: Resource<TitusClusterSpec>): String {
    val tags = runBlocking {
      getTagsForDigest(desired.container, desired.location.account)
    }
    return if (tags.size == 1) {
      tags.first() // only one tag, so use it to deploy
    } else {
      log.debug("Container digest ${desired.container} has multiple tags or no tags: $tags")
      // unclear which "version" to print if there is more than one, so use a shortened version of the digest
      desired.container.digest.subSequence(0, 7).toString()
    }
  }

  override fun getDeployingVersions(resource: Resource<TitusClusterSpec>, diff: ResourceDiff<TitusServerGroup>): Set<String> =
    runBlocking {
      getTagsForDigest(diff.desired.container, diff.desired.location.account)
    }

  override fun correlationId(resource: Resource<TitusClusterSpec>, diff: ResourceDiff<TitusServerGroup>): String =
    "${resource.id}:${diff.desired.location.region}"

  override fun Resource<TitusClusterSpec>.isManagedRollout(): Boolean =
    spec.rolloutWith != null

  override fun Resource<TitusClusterSpec>.regions(): List<String> =
    spec.locations.regions.map { it.name }

  override fun Resource<TitusClusterSpec>.account(): String =
    spec.locations.account

  override fun Resource<TitusClusterSpec>.moniker() =
    spec.moniker

  override fun ResourceDiff<TitusServerGroup>.hasScalingPolicies(): Boolean =
    desired.scaling.hasScalingPolicies()

  override fun ResourceDiff<TitusServerGroup>.isCapacityOrAutoScalingOnly(): Boolean =
    current != null &&
      affectedRootPropertyTypes.all { Capacity::class.java.isAssignableFrom(it) || it == Scaling::class.java } &&
      current!!.scaling.suspendedProcesses == desired.scaling.suspendedProcesses

  override fun ResourceDiff<TitusServerGroup>.hasScalingPolicyDiff(): Boolean =
    current != null && affectedRootPropertyTypes.contains(Scaling::class.java) &&
      (
        current!!.scaling.targetTrackingPolicies != desired.scaling.targetTrackingPolicies ||
          current!!.scaling.stepScalingPolicies != desired.scaling.stepScalingPolicies
        )

  override suspend fun getDisabledServerGroupsByRegion(resource: Resource<TitusClusterSpec>): Map<String, List<TitusServerGroup>> =
    getExistingServerGroupsByRegion(resource)
      .mapValues { regionalList ->
        regionalList.value
          .filter { it.disabled }
          .map { serverGroup: ClouddriverTitusServerGroup ->
          serverGroup.toActive().toTitusServerGroup()
        }
      }

  override fun Resource<TitusClusterSpec>.getDeployWith(): ClusterDeployStrategy =
    spec.deployWith

  override suspend fun export(exportable: Exportable): TitusClusterSpec {
    val serverGroups = cloudDriverService.getActiveServerGroups(
      exportable.account,
      exportable.moniker,
      exportable.regions,
      exportable.user
    ).byRegion()

    if (serverGroups.isEmpty()) {
      throw ResourceNotFound(
        "Could not find cluster: ${exportable.moniker} " +
          "in account: ${exportable.account} for export"
      )
    }

    // let's assume that the largest server group is the most important and should be the base
    val base = serverGroups.values.maxByOrNull { it.capacity.desired }
      ?: throw ExportError("Unable to calculate the server group with the largest capacity from server groups $serverGroups")

    val locations = SimpleLocations(
      account = exportable.account,
      regions = serverGroups.keys.map {
        SimpleRegionSpec(it)
      }.toSet()
    )

    val deployStrategy = clusterExportHelper.discoverDeploymentStrategy(
      cloudProvider = "titus",
      account = exportable.account,
      application = exportable.moniker.app,
      serverGroupName = base.name
    ) ?: RedBlack()

    val spec = TitusClusterSpec(
      moniker = exportable.moniker,
      locations = locations,
      _defaults = base.exportSpec(exportable.moniker.app),
      overrides = mutableMapOf(),
      container = ReferenceProvider(base.container.repository()),
      deployWith = deployStrategy.withDefaultsOmitted()
    )

    spec.generateOverrides(
      serverGroups
        .filter { it.value.location.region != base.location.region }
    )

    return spec
  }

  override suspend fun exportArtifact(exportable: Exportable): DeliveryArtifact {
    val serverGroups = cloudDriverService.getActiveServerGroups(
      exportable.account,
      exportable.moniker,
      exportable.regions,
      exportable.user
    ).byRegion()

    if (serverGroups.isEmpty()) {
      throw ResourceNotFound(
        "Could not find cluster: ${exportable.moniker} " +
          "in account: ${exportable.account} for export"
      )
    }

    val container = serverGroups.values.maxByOrNull { it.capacity.desired }?.container
      ?: throw ExportError("Unable to locate container from the largest server group: $serverGroups")

    val registry = cloudDriverCache.getRegistryForTitusAccount(exportable.account)

    val images = titusRegistryService.findImages(
      image = container.repository(),
      digest = container.digest
    )

    val matchingImage = images.firstOrNull { it.digest == container.digest }
      ?: throw ExportError("Unable to find matching image (searching by digest) in registry ($registry) for $container")

    // check this image was produced by a rocket job
    val commitId = matchingImage.commitId
    val buildNumber = matchingImage.buildNumber
    if (buildNumber != null && commitId != null) {
      if (artifactBridge.getArtifactMetadata(buildNumber, commitId, 1) == null) {
        throw ArtifactNotSupportedException("Failed to fetch information for artifact ${matchingImage.repository}")
      }
    } else {
      throw ArtifactNotSupportedException("Artifact ${matchingImage.repository} is missing commit $commitId and/or build $buildNumber details")
    }


    // prefer branch-based artifact spec, fallback to tag version strategy
    return if (matchingImage.branch != null) {
      DockerArtifact(
        name = container.repository(),
        branch = matchingImage.branch
      )
    } else {
      DockerArtifact(
        name = container.repository(),
        tagVersionStrategy = deriveVersioningStrategy(matchingImage.tag)
          ?: throw DockerArtifactExportError(matchingImage.tag, container.toString())
      )
    }
  }

  fun deriveVersioningStrategy(tag: String): TagVersionStrategy? {
    if (tag.toIntOrNull() != null) {
      return INCREASING_TAG
    }
    if (Regex(SEMVER_JOB_COMMIT_BY_SEMVER.regex).find(tag) != null) {
      return SEMVER_JOB_COMMIT_BY_SEMVER
    }
    if (Regex(BRANCH_JOB_COMMIT_BY_JOB.regex).find(tag) != null) {
      return BRANCH_JOB_COMMIT_BY_JOB
    }
    if (isSemver(tag)) {
      return SEMVER_TAG
    }
    return null
  }

  private fun isSemver(input: String) =
    try {
      SemVer.parse(input.removePrefix("v"))
      true
    } catch (e: IllegalArgumentException) {
      false
    }

  override fun ResourceDiff<TitusServerGroup>.disableOtherServerGroupJob(
    resource: Resource<TitusClusterSpec>,
    desiredVersion: String
  ): Job {
    val current = requireNotNull(current) {
      "Current server group must not be null when generating a disable job"
    }
    val existingServerGroups: Map<String, List<ClouddriverTitusServerGroup>> = runBlocking {
      getExistingServerGroupsByRegion(resource)
    }
    val sgInRegion = existingServerGroups.getOrDefault(current.location.region, emptyList()).filterNot { it.disabled }

    if (sgInRegion.size < 2) {
      log.error(
        "Diff says this is not the only active server group, but now we say otherwise. " +
          "What is going on? Existing server groups: {}", existingServerGroups
      )
      throw ActiveServerGroupsException(resource.id, "No other active server group found to disable.")
    }

    val (rightImageASGs, wrongImageASGs) = sgInRegion
      .sortedBy { it.createdTime }
      .partition { it.image.dockerImageVersion == desiredVersion }

    val sgToDisable = when {
      wrongImageASGs.isNotEmpty() -> {
        log.debug("Disabling oldest server group with incorrect docker image version for {}", resource.id)
        wrongImageASGs.first()
      }
      rightImageASGs.size > 1 -> {
        log.debug(
          "Disabling oldest server group with correct docker image version " +
            "(because there is more than one active server group with the correct image) for {}", resource.id
        )
        rightImageASGs.first()
      }
      else -> {
        log.error("Could not find a server group to disable, looking at: {}", wrongImageASGs + rightImageASGs)
        throw ActiveServerGroupsException(resource.id, "No other active server group found to disable.")
      }
    }
    log.debug("Disabling server group {} for {}: {}", sgToDisable.name, resource.id, sgToDisable)

    return mapOf(
      "type" to "disableServerGroup",
      "cloudProvider" to TITUS_CLOUD_PROVIDER,
      "credentials" to desired.location.account,
      "moniker" to sgToDisable.moniker.orcaClusterMoniker,
      "region" to sgToDisable.region,
      "serverGroupName" to sgToDisable.name,
      "asgName" to sgToDisable.name
    )
  }

  override fun ResourceDiff<TitusServerGroup>.rollbackServerGroupJob(
    resource: Resource<TitusClusterSpec>,
    rollbackServerGroup: TitusServerGroup
  ): Job =
    mutableMapOf(
      "rollbackType" to "EXPLICIT",
      "rollbackContext" to  mapOf(
        "rollbackServerGroupName" to current?.moniker?.serverGroup,
        "restoreServerGroupName" to rollbackServerGroup.moniker.serverGroup,
        "targetHealthyRollbackPercentage" to 100,
        "delayBeforeDisableSeconds" to 0
      ),
      "targetGroups" to desired.dependencies.targetGroups,
      "securityGroups" to desired.dependencies.securityGroupNames,
      "platformHealthOnlyShowOverride" to false,
      "reason" to "rollin' back",
      "type" to "rollbackServerGroup",
      "moniker" to current?.moniker?.orcaClusterMoniker,
      "region" to desired.location.region,
      "credentials" to desired.location.account,
      "cloudProvider" to TITUS_CLOUD_PROVIDER,
      "user" to DEFAULT_SERVICE_ACCOUNT
    )

  override fun ResourceDiff<TitusServerGroup>.resizeServerGroupJob(): Job {
    val current = requireNotNull(current) {
      "Current server group must not be null when generating a resize job"
    }
    return mapOf(
      "refId" to "1",
      "type" to "resizeServerGroup",
      "capacity" to mapOf(
        "min" to desired.capacity.min,
        "max" to desired.capacity.max,
        "desired" to resolveDesiredCapacity()
      ),
      "cloudProvider" to TITUS_CLOUD_PROVIDER,
      "credentials" to desired.location.account,
      "moniker" to current.moniker.orcaClusterMoniker,
      "region" to current.location.region,
      "serverGroupName" to current.name
    )
  }

  /**
   * @return list of stages to remove or create scaling policies in-place on the
   * current serverGroup.
   *
   * Scaling policies are treated as immutable by keel once applied. If an existing
   * policy is modified, it will be deleted and reapplied via a single task.
   */
  override fun ResourceDiff<TitusServerGroup>.modifyScalingPolicyJob(startingRefId: Int): List<Job> {
    var (refId, stages) = toDeletePolicyJob(startingRefId)

    val newTargetPolicies = current?.run {
      desired.scaling.targetTrackingPolicies - desired.scaling.targetTrackingPolicies.intersectingConfigurations(scaling.targetTrackingPolicies)
    } ?: desired.scaling.targetTrackingPolicies

    log.debug("Target tracking policies for resource ${desired.id}:\nNew: $newTargetPolicies.\nExisting: ${current?.scaling?.targetTrackingPolicies ?: "None"}")

    val newStepPolicies = current?.run {
      desired.scaling.stepScalingPolicies - desired.scaling.stepScalingPolicies.intersectingConfigurations(scaling.stepScalingPolicies)
    } ?: desired.scaling.stepScalingPolicies

    log.debug("Step scaling policies for resource ${desired.id}:\nNew: $newStepPolicies.\nExisting: ${current?.scaling?.stepScalingPolicies ?: "None"}")

    if (newTargetPolicies.isNotEmpty()) {
      val (newRef, jobs) = newTargetPolicies.toCreateTargetTrackingPolicyJob(refId, current ?: desired)
      refId = newRef
      stages.addAll(jobs)
    }

    if (newStepPolicies.isNotEmpty()) {
      stages.addAll(newStepPolicies.toCreateStepPolicyJob(refId, current ?: desired))
    }

    return stages
  }

  private fun ResourceDiff<TitusServerGroup>.toDeletePolicyJob(startingRefId: Int): Pair<Int, MutableList<Job>> {
    var refId = startingRefId
    val stages = mutableListOf<Job>()
    if (current == null) {
      return Pair(refId, stages)
    }
    val current = current!!
    val targetPoliciesToRemove = current.scaling.targetTrackingPolicies.notPresentOrDuplicatedIn(desired.scaling.targetTrackingPolicies)
    val stepPoliciesToRemove = current.scaling.stepScalingPolicies.notPresentOrDuplicatedIn(desired.scaling.stepScalingPolicies)
    val policyNamesToRemove = targetPoliciesToRemove.mapNotNull { it.name } +
      stepPoliciesToRemove.mapNotNull { it.name }
        .toSet()

    stages.addAll(
      policyNamesToRemove
        .map {
          refId++
          mapOf(
            "refId" to refId.toString(),
            "requisiteStageRefIds" to when (refId) {
              0, 1 -> listOf()
              else -> listOf((refId - 1).toString())
            },
            "type" to "deleteScalingPolicy",
            "scalingPolicyID" to it,
            "cloudProvider" to TITUS_CLOUD_PROVIDER,
            "credentials" to desired.location.account,
            "moniker" to current.moniker.orcaClusterMoniker,
            "region" to current.location.region,
            "serverGroupName" to current.moniker.serverGroup
          )
        }
        .toMutableList()
    )

    return Pair(refId, stages)
  }

  private fun Set<TargetTrackingPolicy>.toCreateTargetTrackingPolicyJob(
    startingRefId: Int,
    serverGroup: TitusServerGroup
  ): Pair<Int, List<Job>> {
    var refId = startingRefId
    val stages = map {
      refId++
      mapOf(
        "refId" to refId.toString(),
        "requisiteStageRefIds" to when (refId) {
          0, 1 -> emptyList<String>()
          else -> listOf((refId - 1).toString())
        },
        "type" to "upsertScalingPolicy",
        "jobId" to serverGroup.id,
        "cloudProvider" to TITUS_CLOUD_PROVIDER,
        "credentials" to serverGroup.location.account,
        "moniker" to serverGroup.moniker.orcaClusterMoniker,
        "region" to serverGroup.location.region,
        "serverGroupName" to serverGroup.moniker.serverGroup,
        "targetTrackingConfiguration" to mapOf(
          "targetValue" to it.targetValue,
          "disableScaleIn" to it.disableScaleIn,
          "scaleOutCooldown" to it.scaleOutCooldown?.seconds,
          "scaleInCooldown" to it.scaleInCooldown?.seconds,
          "predefinedMetricSpecification" to when (val metricsSpec = it.predefinedMetricSpec) {
            null -> null
            else -> with(metricsSpec) {
              PredefinedMetricSpecificationModel(
                predefinedMetricType = type,
                resourceLabel = label
              )
            }
          },
          "customizedMetricSpecification" to when (val metricsSpec = it.customMetricSpec) {
            null -> null
            else -> with(metricsSpec) {
              CustomizedMetricSpecificationModel(
                metricName = name,
                namespace = namespace,
                statistic = statistic,
                unit = unit,
                dimensions = (
                  dimensions?.map { d ->
                    MetricDimensionModel(name = d.name, value = d.value)
                  } ?: emptyList()
                ) + MetricDimensionModel("AutoScalingGroupName", serverGroup.name)
              )
            }
          }
        )
      )
    }

    return Pair(refId, stages)
  }

  private fun Set<StepScalingPolicy>.toCreateStepPolicyJob(
    startingRefId: Int,
    serverGroup: TitusServerGroup
  ): List<Job> {
    var refId = startingRefId
    return map {
      refId++
      mapOf(
        "refId" to refId.toString(),
        "requisiteStageRefIds" to when (refId) {
          0, 1 -> emptyList<String>()
          else -> listOf((refId - 1).toString())
        },
        "type" to "upsertScalingPolicy",
        "jobId" to serverGroup.id,
        "cloudProvider" to TITUS_CLOUD_PROVIDER,
        "credentials" to serverGroup.location.account,
        "moniker" to serverGroup.moniker.orcaClusterMoniker,
        "region" to serverGroup.location.region,
        "adjustmentType" to it.adjustmentType,
        "alarm" to mapOf(
          "region" to serverGroup.location.region,
          "actionsEnabled" to it.actionsEnabled,
          "comparisonOperator" to it.comparisonOperator,
          "dimensions" to it.dimensions,
          "evaluationPeriods" to it.evaluationPeriods,
          "period" to it.period.seconds,
          "threshold" to it.threshold,
          "namespace" to it.namespace,
          "metricName" to it.metricName,
          "statistic" to it.statistic
        ),
        "step" to mapOf(
          "metricAggregationType" to it.metricAggregationType,
          "stepAdjustments" to it.stepAdjustments.map { adjustment ->
            StepAdjustmentModel(
              metricIntervalLowerBound = adjustment.lowerBound,
              metricIntervalUpperBound = adjustment.upperBound,
              scalingAdjustment = adjustment.scalingAdjustment
            )
          }
        )
      )
    }
  }

  private fun ResourceDiff<TitusServerGroup>.generateImageJson(
    tag: String?
  ) =
    with(desired) {
      if (tag == null || desired.container.digest.startsWith(tag)) {
        mapOf(
          "digest" to container.digest,
          "imageId" to "${container.organization}/${container.image}:${container.digest}"
        )
      } else {
        mapOf(
          "tag" to tag,
          "imageId" to "${container.organization}/${container.image}:$tag"
        )
      }
    }

  /**
   * If a tag is provided, deploys by tag.
   * Otherwise, deploys by digest.
   */
  override fun ResourceDiff<TitusServerGroup>.upsertOrCloneServerGroupJob(
    resource: Resource<TitusClusterSpec>,
    startingRefId: Int,
    version: String?,
    clone: Boolean
  ) =
    with(desired) {
      val image = generateImageJson(version)

      mapOf(
        "refId" to (startingRefId + 1).toString(),
        "requisiteStageRefIds" to when (startingRefId) {
          0 -> emptyList()
          else -> listOf(startingRefId.toString())
        },
        "application" to moniker.app,
        "credentials" to location.account,
        "region" to location.region,
        "network" to "default",
        "inService" to true,
        "capacity" to mapOf(
          "min" to capacity.min,
          "max" to capacity.max,
          "desired" to resolveDesiredCapacity()
        ),
        "targetHealthyDeployPercentage" to 100, // TODO: any reason to do otherwise?
        "useDefaultIamRole" to true,
        // <titus things>
        "capacityGroup" to capacityGroup,
        "entryPoint" to entryPoint,
        "env" to env,
        "containerAttributes" to containerAttributes,
        "constraints" to constraints,
        "registry" to runBlocking { cloudDriverCache.getRegistryForTitusAccount(location.account) },
        "migrationPolicy" to migrationPolicy,
        "resources" to resources,
        // </titus things>
        "stack" to moniker.stack,
        "freeFormDetails" to moniker.detail,
        "tags" to tags,
        "moniker" to moniker.orcaClusterMoniker,
        "reason" to if (clone) "Redeploy ${image["imageId"]}" else "Diff detected at ${clock.instant().iso()}",
        "type" to if (clone) "cloneServerGroup" else "createServerGroup",
        "cloudProvider" to TITUS_CLOUD_PROVIDER,
        "securityGroups" to if (clone) securityGroupIdsForClone() else securityGroupIds(),
        "loadBalancers" to dependencies.loadBalancerNames,
        "targetGroups" to dependencies.targetGroups,
        "account" to location.account,
        "efs" to efs,
        "platformSidecars" to platformSidecars.map {
          mapOf(
            "name" to it.name,
            "channel" to it.channel,
            // Titus expects a json object encoded as a string for "arguments"
            "arguments" to jsonStringify(it.arguments)
          )
        },
        "networkMode" to networkMode?.name
        // scaling is not set here because the deploy stage does not accept scaling policies.
        // They're copied on a clone if they exist, or created in a modify job by us if they don't.
      ) + image
    }
      .let { job ->
        current?.run {
          job + mapOf(
            "source" to mapOf(
              "account" to location.account,
              "region" to location.region,
              "asgName" to moniker.serverGroup
            )
          )
        } ?: job
      }
      .let { job ->
        // add deployment strategy properties
        job + resource.spec.deployWith.toOrcaJobProperties("Titus")
      }
      .let { job ->
        // add metadata
        job + mapOf("metadata" to mapOf("resource" to resource.id))
      }

/**
 * Convert a map to string-encoded JSON
 *
 * Note: null is treated as a special case.
 * If the input is null, the output is null, not "null"
 *
 */
private fun jsonStringify(arguments: Map<String, Any>?) =
   arguments?.let { args -> jacksonObjectMapper().writeValueAsString(args) }

  /**
   * For server groups with scaling policies, the [TitusClusterSpec] will not include a desired value. so we use the
   * higher of the desired value the server group we're replacing uses, or the max. This means we won't catastrophically
   * down-size a server group by deploying it.
   */
  private fun ResourceDiff<TitusServerGroup>.resolveDesiredCapacity() =
    when (desired.capacity) {
      // easy case: spec supplied the desired value as there are no scaling policies in effect
      is DefaultCapacity -> desired.capacity.desired
      // scaling policies exist, so use a safe value
      is AutoScalingCapacity -> maxOf(current?.capacity?.desired ?: 0, desired.capacity.max)
    }

  override fun Resource<TitusClusterSpec>.upsertServerGroupManagedRolloutJob(
    diffs: List<ResourceDiff<TitusServerGroup>>,
    version: String?
  ): Job {
    val image = diffs.first().generateImageJson(version) // image json should be the same for all regions.

    return mapOf(
      "refId" to "1",
      "type" to "managedRollout",
      "input" to mapOf(
        "selectionStrategy" to spec.rolloutWith?.strategy?.type?.enumStyleName,
        "targets" to spec.generateRolloutTargets(diffs),
        "clusterDefinitions" to listOf(toManagedRolloutClusterDefinition(image, diffs))
      ),
      "reason" to "Diff detected at ${clock.instant().iso()}",
    )
  }

  private fun Resource<TitusClusterSpec>.toManagedRolloutClusterDefinition(image: Map<String, Any>, diffs: List<ResourceDiff<TitusServerGroup>>) =
    with(spec) {
      val dependencies = resolveDependencies()
      mapOf(
        "application" to moniker.app,
        "stack" to moniker.stack,
        "freeFormDetails" to moniker.detail,
        "inService" to true,
        "targetHealthyDeployPercentage" to 100, // TODO: any reason to do otherwise?
        "cloudProvider" to TITUS_CLOUD_PROVIDER,
        "network" to "default",
        "registry" to runBlocking { cloudDriverCache.getRegistryForTitusAccount(locations.account) },
        "capacity" to resolveCapacity(),
        "capacityGroup" to resolveCapacityGroup(),
        "securityGroups" to dependencies.securityGroupNames,
        "loadBalancers" to dependencies.loadBalancerNames,
        "targetGroups" to dependencies.targetGroups,
        "entryPoint" to resolveEntryPoint(),
        "env" to resolveEnv(),
        "containerAttributes" to resolveContainerAttributes(),
        "tags" to resolveTags(),
        "resources" to resolveResources(),
        "constraints" to resolveConstraints(),
        "migrationPolicy" to resolveMigrationPolicy(),
        "platformSidecars" to resolvePlatformSidecars()
        // scaling is not set here because the deploy stage does not accept scaling policies.
        // They're copied on a clone if they exist, or created in a modify job by us if they don't.
      ) + image +
        mapOf("overrides" to buildOverrides(diffs)) +
        spec.deployWith.toOrcaJobProperties("Titus")
    }

  fun Resource<TitusClusterSpec>.buildOverrides(diffs: List<ResourceDiff<TitusServerGroup>>): Map<String, Any?> {
    val overrides: MutableMap<String, Any?> = spec.overrides.toMutableMap()
    diffs.forEach { diff ->
      val region = getDesiredRegion(diff)
      val existingOverride: MutableMap<String, Any?> = mapper.convertValue(overrides[region] ?: mutableMapOf<String, Any?>())
      existingOverride["dependencies"]?.let { dependencyOverrides ->
        // need to remove nesting of these keys, plus change the name of some of them
        val dependencies: ClusterDependencies = mapper.convertValue(dependencyOverrides)
        if (dependencies.loadBalancerNames.isNotEmpty()) {
          existingOverride["loadBalancers"] = dependencies.loadBalancerNames
        }
        if (dependencies.securityGroupNames.isNotEmpty()) {
          existingOverride["securityGroups"] = dependencies.securityGroupNames
        }
        if(dependencies.targetGroups.isNotEmpty()) {
          existingOverride["targetGroups"] = dependencies.targetGroups
        }
      }
      // todo eb: are there more things that get touched by the resolvers that we need to add?
      // How can we go from resolvers to the cluster spec with changes?
      val containerAttributes: Map<String, String> = mapper.convertValue(diff.desired.containerAttributes)
      val environmentVariables: Map<String, String> = mapper.convertValue(diff.desired.env)
      overrides[region] = existingOverride + mapOf("containerAttributes" to containerAttributes) + mapOf("env" to environmentVariables)
    }
    return overrides
  }

  private fun TitusClusterSpec.generateRolloutTargets(diffs: List<ResourceDiff<TitusServerGroup>>): List<Map<String, Any>> =
    diffs
      .map {
        val region = getDesiredRegion(it)
        mapper.convertValue(
          RolloutTarget(
            TITUS_CLOUD_PROVIDER,
            RolloutLocation(
              locations.account,
              getDesiredRegion(it),
              emptyList()
            ),
            rolloutWith?.getOrder(region),
            rolloutWith?.getPostDeployWait(region)
          )
        )
      }

  /**
   * @return `true` if the only changes in the diff are to capacity.
   */
  override fun ResourceDiff<TitusServerGroup>.isCapacityOnly(): Boolean =
    current != null && affectedRootPropertyTypes.all { Capacity::class.java.isAssignableFrom(it) }

  /**
   * @return `true` if the only changes in the diff are to scaling.
   */
  override fun ResourceDiff<TitusServerGroup>.isAutoScalingOnly(): Boolean =
    current != null &&
      affectedRootPropertyTypes.any { it == Scaling::class.java } &&
      affectedRootPropertyTypes.all { Capacity::class.java.isAssignableFrom(it) || it == Scaling::class.java } &&
      current!!.capacity.min == desired.capacity.min &&
      current!!.capacity.max == desired.capacity.max

  /**
   * @return true if the only difference is in the onlyEnabledServerGroup property
   */
  override fun ResourceDiff<TitusServerGroup>.isEnabledOnly(): Boolean =
    current != null &&
      affectedRootPropertyNames.all { it == "onlyEnabledServerGroup" } &&
      current!!.onlyEnabledServerGroup != desired.onlyEnabledServerGroup

  private fun TitusClusterSpec.generateOverrides(serverGroups: Map<String, TitusServerGroup>) =
    serverGroups.forEach { (region, serverGroup) ->
      val workingSpec = serverGroup.exportSpec(moniker.app)
      val override: TitusServerGroupSpec? = buildSpecFromDiff(defaults, workingSpec)
      if (override != null) {
        (overrides as MutableMap)[region] = override
      }
    }

  private suspend fun CloudDriverService.getActiveServerGroups(resource: Resource<TitusClusterSpec>): Iterable<TitusServerGroup> {
    val existingServerGroups: Map<String, List<ClouddriverTitusServerGroup>> = getExistingServerGroupsByRegion(resource)
    val activeServerGroups = getActiveServerGroups(
      resource.spec.locations.account,
      resource.spec.moniker,
      resource.spec.locations.regions.map { it.name }.toSet(),
      resource.serviceAccount
    ).map { activeServerGroup ->
      if (resource.spec.deployWith is NoStrategy) {
        // we only care about num enabled if there is a deploy strategy
        // if there is no deploy strategy, ignore the calculation so that there isn't a diff ever in this property.
        activeServerGroup.copy(onlyEnabledServerGroup = true)
      } else {
        val numEnabled = existingServerGroups
          .getOrDefault(activeServerGroup.location.region, emptyList<ServerGroup>())
          .filter { !it.disabled }
          .size

        when (numEnabled) {
          1 -> activeServerGroup.copy(onlyEnabledServerGroup = true)
          else -> activeServerGroup.copy(onlyEnabledServerGroup = false)
        }
      }
    }

    return activeServerGroups
  }

  override suspend fun getExistingServerGroupsByRegion(resource: Resource<TitusClusterSpec>): Map<String, List<ClouddriverTitusServerGroup>> {
    val existingServerGroups: MutableMap<String, MutableList<ClouddriverTitusServerGroup>> = mutableMapOf()

    try {
      cloudDriverService
        .listTitusServerGroups(
          user = resource.serviceAccount,
          app = resource.application,
          account = resource.spec.locations.account,
          cluster = resource.spec.moniker.toString()
        )
        .serverGroups
        .forEach { sg ->
          val existing = existingServerGroups.getOrPut(sg.region) { mutableListOf() }
          existing.add(sg)
          existingServerGroups[sg.region] = existing
        }
    } catch (e: HttpException) {
      if (!e.isNotFound) {
        throw e
      }
    }
    return existingServerGroups
  }

  /**
   * Get all tags that match a digest, filtering out the "latest" tag.
   * Note: there may be more than one tag with the same digest
   */
  private suspend fun getTagsForDigest(container: DigestProvider, titusAccount: String): Set<String> =
    titusRegistryService.findImages(
      titusAccount = titusAccount,
      image = container.repository(),
      digest = container.digest
    )
      .filter { it.digest == container.digest && it.tag != "latest" }
      .map { it.tag }
      .toSet()

  private suspend fun CloudDriverService.getActiveServerGroups(
    account: String,
    moniker: Moniker,
    regions: Set<String>,
    serviceAccount: String
  ): Iterable<TitusServerGroup> =
    coroutineScope {
      regions.map {
        async {
          try {
            titusActiveServerGroup(
              serviceAccount,
              moniker.app,
              account,
              moniker.toString(),
              it,
              TITUS_CLOUD_PROVIDER
            )
              .toTitusServerGroup()
          } catch (e: HttpException) {
            if (!e.isNotFound) {
              throw e
            }
            null
          }
        }
      }
        .mapNotNull { it.await() }
    }

  private fun TitusActiveServerGroup.toTitusServerGroup() =
    TitusServerGroup(
      id = id,
      name = name,
      location = Location(
        account = placement.account,
        region = region
      ),
      capacity = capacity.run {
        if (scalingPolicies.isEmpty()) {
          DefaultCapacity(min, max, desired)
        } else {
          AutoScalingCapacity(min, max, desired)
        }
      },
      container = DigestProvider(
        organization = image.dockerImageName.split("/").first(),
        image = image.dockerImageName.split("/").last(),
        digest = image.dockerImageDigest
      ),
      entryPoint = entryPoint,
      resources = resources.run { Resources(cpu, disk, gpu, memory, networkMbps) },
      env = env,
      containerAttributes = containerAttributes,
      constraints = constraints.run { Constraints(hard, soft) },
      capacityGroup = capacityGroup,
      migrationPolicy = migrationPolicy.run { MigrationPolicy(type) },
      dependencies = ClusterDependencies(
        loadBalancers,
        securityGroupNames = securityGroupNames,
        targetGroups = targetGroups
      ),
      instanceCounts = instanceCounts.run { InstanceCounts(total, up, down, unknown, outOfService, starting) },
      scaling = Scaling(
        targetTrackingPolicies = scalingPolicies.filter { it.policy is TargetPolicy }.mapUnique { policy ->
          with((policy.policy as TargetPolicy).targetPolicyDescriptor) {
            TargetTrackingPolicy(
              name = policy.id,
              warmup = null,
              targetValue = targetValue,
              disableScaleIn = disableScaleIn,
              scaleInCooldown = Duration.ofSeconds(scaleInCooldownSec.toLong()),
              scaleOutCooldown = Duration.ofSeconds(scaleOutCooldownSec.toLong()),
              customMetricSpec = customizedMetricSpecification?.let { metric ->
                CustomizedMetricSpecification(
                  name = metric.metricName,
                  namespace = metric.namespace,
                  statistic = metric.statistic,
                  unit = metric.unit,
                  dimensions = metric
                    .dimensions
                    ?.filter { it.name !in IGNORED_SCALING_DIMENSIONS }
                    ?.mapUnique {
                      MetricDimension(
                        name = it.name,
                        value = it.value
                      )
                    }
                )
              }
            )
          }
        },
        stepScalingPolicies = scalingPolicies.filter { it.policy is StepPolicy }.mapUnique { policy ->
          with((policy.policy as StepPolicy).stepPolicyDescriptor) {
            StepScalingPolicy(
              name = policy.id,
              adjustmentType = scalingPolicy.adjustmentType,
              actionsEnabled = true,
              comparisonOperator = alarmConfig.comparisonOperator,
              dimensions = emptySet(), // Titus doesn't support dimensions on step policies yet
              evaluationPeriods = alarmConfig.evaluationPeriods,
              period = Duration.ofSeconds(alarmConfig.periodSec.toLong()),
              threshold = alarmConfig.threshold.toInt(),
              metricName = alarmConfig.metricName,
              namespace = alarmConfig.metricNamespace,
              statistic = alarmConfig.statistic,
              warmup = ZERO,
              metricAggregationType = scalingPolicy.metricAggregationType,
              stepAdjustments = scalingPolicy.stepAdjustments.mapUnique {
                StepAdjustment(
                  lowerBound = it.metricIntervalLowerBound,
                  upperBound = it.metricIntervalUpperBound,
                  scalingAdjustment = it.scalingAdjustment
                )
              }
            )
          }
        }
      ),
      efs = efs?.let {
        ElasticFileSystem(
          mountPerm = it.mountPerm,
          mountPoint = it.mountPoint,
          efsId = it.efsId,
          efsRelativeMountPoint = it.efsRelativeMountPoint,
        )
      },
      platformSidecars = platformSidecars.map {
        TitusServerGroup.PlatformSidecar(it.name, it.channel, it.arguments)
      },
      networkMode = networkMode?.runCatching(NetworkMode::valueOf)?.getOrNull()
    )

  private fun TitusServerGroup.securityGroupIds(): Collection<String> =
    runBlocking {
      val awsAccount = cloudDriverCache.getAwsAccountNameForTitusAccount(location.account)
      dependencies
        .securityGroupNames
        // no need to specify these as Orca will auto-assign them, also the application security group
        // gets auto-created so may not exist yet
        .filter { it !in setOf("nf-infrastructure", "nf-datacenter", moniker.app) }
        .map { cloudDriverCache.securityGroupByName(awsAccount, location.region, it).id }
    }

  private fun TitusServerGroup.securityGroupIdsForClone(): Collection<String> =
    runBlocking {
      val awsAccount = cloudDriverCache.getAwsAccountNameForTitusAccount(location.account)
      dependencies
        .securityGroupNames
        .map { cloudDriverCache.securityGroupByName(awsAccount, location.region, it).id }
    }

  private val TitusActiveServerGroup.securityGroupNames: Set<String>
    get() = securityGroups.map {
      cloudDriverCache.securityGroupById(awsAccount, region, it).name
    }
      .toSet()

  private fun Instant.iso() =
    atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_DATE_TIME)

  private fun TitusServerGroup.exportSpec(application: String): TitusServerGroupSpec {
    val defaults = TitusServerGroupSpec(
      capacity = CapacitySpec(1, 1, 1),
      iamProfile = application + "InstanceProfile",
      resources = mapper.convertValue(Resources()),
      entryPoint = "",
      constraints = Constraints(),
      migrationPolicy = MigrationPolicy(),
      dependencies = ClusterDependencies(),
      capacityGroup = application,
      env = emptyMap(),
      containerAttributes = emptyMap(),
      tags = emptyMap(),
      scaling = TitusScalingSpec(),
    )

    val thisSpec = TitusServerGroupSpec(
      capacity = capacity.run { CapacitySpec(min, max, desired) },
      capacityGroup = capacityGroup,
      constraints = constraints,
      dependencies = dependencies,
      entryPoint = entryPoint,
      env = env.filterNot { EXPORT_IGNORED_ENV.contains(it.key) },
      containerAttributes = containerAttributes.apply {
        // We should ignore [EXPORT_IGNORED_CONTAINER_ATTRIBUTES] keys in the prod and test envs
        if ((EXPORT_IGNORED_CONTAINER_ATTRIBUTES_ACCOUNTS.contains(location.account))) {
          filterNot { EXPORT_IGNORED_CONTAINER_ATTRIBUTES.contains(it.key) }
        }
      },
      migrationPolicy = migrationPolicy,
      resources = resources.toSpec(),
      tags = tags,
      scaling = if (scaling.hasScalingPolicies()) scaling.toTitusScalingSpec() else null,
      networkMode = networkMode
    )

    return checkNotNull(buildSpecFromDiff(defaults, thisSpec))
  }

  private fun Scaling.toTitusScalingSpec() =
    TitusScalingSpec(
      targetTrackingPolicies = targetTrackingPolicies,
      stepScalingPolicies = stepScalingPolicies
    )

  private fun Resources.toSpec(): ResourcesSpec =
    ResourcesSpec(
      cpu = cpu,
      disk = disk,
      gpu = gpu,
      memory = memory,
      networkMbps = networkMbps
    )

  private inline fun <T, R> Collection<T>.mapUnique(transform: (T) -> R): Set<R> = mapTo(HashSet(size), transform)

  companion object {
    private val IGNORED_SCALING_DIMENSIONS = listOf("AutoScalingGroupName")
    private val EXPORT_IGNORED_ENV = listOf("EC2_REGION", "NETFLIX_REGION", "SPINNAKER_ACCOUNT", "NETFLIX_HOME_REGION")
    private val EXPORT_IGNORED_CONTAINER_ATTRIBUTES = listOf("titusParameter.agent.accountId", "titusParameter.agent.subnets")
    private val EXPORT_IGNORED_CONTAINER_ATTRIBUTES_ACCOUNTS = listOf("titusprodvpc", "titustestvpc")
  }
}
