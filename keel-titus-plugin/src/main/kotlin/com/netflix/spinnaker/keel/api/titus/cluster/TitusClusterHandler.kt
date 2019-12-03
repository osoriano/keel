/*
 *
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.spinnaker.keel.api.titus.cluster

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.keel.api.Capacity
import com.netflix.spinnaker.keel.api.ClusterDependencies
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.id
import com.netflix.spinnaker.keel.api.Exportable
import com.netflix.spinnaker.keel.api.SimpleLocations
import com.netflix.spinnaker.keel.api.SubmittedResource
import com.netflix.spinnaker.keel.api.SimpleRegionSpec
import com.netflix.spinnaker.keel.api.serviceAccount
import com.netflix.spinnaker.keel.api.titus.CLOUD_PROVIDER
import com.netflix.spinnaker.keel.api.titus.SPINNAKER_TITUS_API_V1
import com.netflix.spinnaker.keel.api.titus.exceptions.RegistryNotFoundException
import com.netflix.spinnaker.keel.api.titus.exceptions.TitusAccountConfigurationException
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.clouddriver.ResourceNotFound
import com.netflix.spinnaker.keel.clouddriver.model.Resources
import com.netflix.spinnaker.keel.clouddriver.model.TitusActiveServerGroup
import com.netflix.spinnaker.keel.diff.ResourceDiff
import com.netflix.spinnaker.keel.events.Task
import com.netflix.spinnaker.keel.model.Moniker
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.plugin.TaskLauncher
import com.netflix.spinnaker.keel.plugin.Resolver
import com.netflix.spinnaker.keel.plugin.ResourceHandler
import com.netflix.spinnaker.keel.plugin.SupportedKind
import com.netflix.spinnaker.keel.retrofit.isNotFound
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.springframework.context.ApplicationEventPublisher
import retrofit2.HttpException
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

class TitusClusterHandler(
  private val cloudDriverService: CloudDriverService,
  private val cloudDriverCache: CloudDriverCache,
  private val orcaService: OrcaService,
  private val clock: Clock,
  private val taskLauncher: TaskLauncher,
  private val publisher: ApplicationEventPublisher,
  objectMapper: ObjectMapper,
  resolvers: List<Resolver<*>>
) : ResourceHandler<TitusClusterSpec, Map<String, TitusServerGroup>>(objectMapper, resolvers) {

  override val supportedKind =
    SupportedKind(SPINNAKER_TITUS_API_V1, "cluster", TitusClusterSpec::class.java)

  override suspend fun toResolvedType(resource: Resource<TitusClusterSpec>): Map<String, TitusServerGroup> =
    with(resource.spec) {
      resolve().byRegion()
    }

  override suspend fun current(resource: Resource<TitusClusterSpec>): Map<String, TitusServerGroup> =
    cloudDriverService
      .getServerGroups(resource)
      .byRegion()

  override suspend fun <T : ResourceSpec> actuationInProgress(resource: Resource<T>) =
    (resource.spec as TitusClusterSpec).locations
      .regions
      .map { it.name }
      .any { region ->
        orcaService
          .getCorrelatedExecutions("${resource.id}:$region")
          .isNotEmpty()
      }

  override suspend fun upsert(
    resource: Resource<TitusClusterSpec>,
    resourceDiff: ResourceDiff<Map<String, TitusServerGroup>>
  ): List<Task> =
    coroutineScope {
      resourceDiff
        .toIndividualDiffs()
        .filter { diff -> diff.hasChanges() }
        .map { diff ->
          val desired = diff.desired
          val job = when {
            diff.isCapacityOnly() -> diff.resizeServerGroupJob()
            else -> diff.upsertServerGroupJob() + resource.spec.deployWith.toOrcaJobProperties()
          }

          log.info("Upserting server group using task: {}", job)

          async {
            taskLauncher.submitJobToOrca(
              resource = resource,
              description = "Upsert server group ${desired.moniker.name} in ${desired.location.account}/${desired.location.region}",
              correlationId = "${resource.id}:${desired.location.region}",
              job = job
            )
          }
        }
        .map { it.await() }
    }

  override suspend fun export(exportable: Exportable): SubmittedResource<TitusClusterSpec> {
    // TODO[GY] : remove unchanged/default fields from response (across all export responses)
    val serverGroups = cloudDriverService.getServerGroups(
      exportable.account,
      exportable.moniker,
      exportable.regions,
      exportable.serviceAccount
    ).byRegion()

    if (serverGroups.isEmpty()) {
      throw ResourceNotFound("Could not find cluster: ${exportable.moniker.name} " +
        "in account: ${exportable.account} for export")
    }

    val base = serverGroups.values.first()

    val locations = SimpleLocations(
      account = exportable.account,
      regions = (serverGroups.keys.map {
        SimpleRegionSpec(it)
      }).toSet())

    val spec = TitusClusterSpec(
      moniker = exportable.moniker,
      locations = locations,
      _defaults = base.exportSpec(),
      overrides = mutableMapOf()
    )

    spec.generateOverrides(
      serverGroups
        .filter { it.value.location.region != base.location.region }
    )

    return SubmittedResource(
      apiVersion = supportedKind.apiVersion,
      kind = supportedKind.kind,
      spec = spec,
      metadata = mapOf("serviceAccount" to exportable.serviceAccount)
    )
  }

  private fun ResourceDiff<TitusServerGroup>.resizeServerGroupJob(): Map<String, Any?> {
    val current = requireNotNull(current) {
      "Current server group must not be null when generating a resize job"
    }
    return mapOf(
      "type" to "resizeServerGroup",
      "capacity" to mapOf(
        "min" to desired.capacity.min,
        "max" to desired.capacity.max,
        "desired" to desired.capacity.desired
      ),
      "cloudProvider" to CLOUD_PROVIDER,
      "credentials" to desired.location.account,
      "moniker" to mapOf(
        "app" to current.moniker.app,
        "stack" to current.moniker.stack,
        "detail" to current.moniker.detail,
        "cluster" to current.moniker.name,
        "sequence" to current.moniker.sequence
      ),
      "region" to current.location.region,
      "serverGroupName" to current.name
    )
  }

  private fun ResourceDiff<TitusServerGroup>.upsertServerGroupJob(): Map<String, Any?> =
    with(desired) {
      mapOf(
        "application" to moniker.app,
        "credentials" to location.account,
        "region" to location.region,
        "network" to "default",
        // todo: does 30 minutes then rollback make sense?
        "stageTimeoutMs" to Duration.ofMinutes(30).toMillis(),
        "inService" to true,
        "capacity" to mapOf(
          "min" to capacity.min,
          "max" to capacity.max,
          "desired" to capacity.desired
        ),
        "targetHealthyDeployPercentage" to 100, // TODO: any reason to do otherwise?
        "iamProfile" to iamProfile,
        // <titus things>
        "capacityGroup" to capacityGroup,
        "entryPoint" to entryPoint,
        "env" to env,
        "constraints" to constraints,
        "digest" to container.digest,
        "registry" to runBlocking { getRegistryForTitusAccount(location.account) },
        "migrationPolicy" to migrationPolicy,
        "resources" to resources,
        "imageId" to "${container.organization}/${container.image}:${container.digest}",
        // </titus things>
        "stack" to moniker.stack,
        "freeFormDetails" to moniker.detail,
        "tags" to tags,
        "moniker" to mapOf(
          "app" to moniker.app,
          "stack" to moniker.stack,
          "detail" to moniker.detail,
          "cluster" to moniker.name
        ),
        "reason" to "Diff detected at ${clock.instant().iso()}",
        "type" to if (current == null) "createServerGroup" else "upsertServerGroup",
        "cloudProvider" to CLOUD_PROVIDER,
        "securityGroups" to securityGroupIds(),
        "loadBalancers" to dependencies.loadBalancerNames,
        "targetGroups" to dependencies.targetGroups,
        "account" to location.account
      )
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

  /**
   * @return `true` if the only changes in the diff are to capacity.
   */
  private fun ResourceDiff<TitusServerGroup>.isCapacityOnly(): Boolean =
    current != null && affectedRootPropertyTypes.all { it == Capacity::class.java }

  private fun ResourceDiff<Map<String, TitusServerGroup>>.toIndividualDiffs() =
    desired
      .map { (region, desired) ->
        ResourceDiff(desired, current?.get(region))
      }

  private fun TitusClusterSpec.generateOverrides(serverGroups: Map<String, TitusServerGroup>) =
    serverGroups.forEach { (region, serverGroup) ->
      var newSpec = TitusServerGroupSpec()
      val workingSpec = serverGroup.exportSpec()
      val diff = ResourceDiff(workingSpec, defaults)
      if (diff.hasChanges()) {
        if ("capacity" in diff.affectedRootPropertyNames) {
          newSpec = newSpec.copy(capacity = workingSpec.capacity)
        }
        if ("capacityGroup" in diff.affectedRootPropertyNames) {
          newSpec = newSpec.copy(capacityGroup = workingSpec.capacityGroup)
        }
        if ("constraints" in diff.affectedRootPropertyNames) {
          newSpec = newSpec.copy(constraints = workingSpec.constraints)
        }
        if ("container" in diff.affectedRootPropertyNames) {
          newSpec = newSpec.copy(container = workingSpec.container)
        }
        if ("dependencies" in diff.affectedRootPropertyNames) {
          newSpec = newSpec.copy(dependencies = workingSpec.dependencies)
        }
        if ("entryPoint" in diff.affectedRootPropertyNames) {
          newSpec = newSpec.copy(entryPoint = workingSpec.entryPoint)
        }
        if ("env" in diff.affectedRootPropertyNames) {
          newSpec = newSpec.copy(env = workingSpec.env)
        }
        if ("iamProfile" in diff.affectedRootPropertyNames) {
          newSpec = newSpec.copy(iamProfile = workingSpec.iamProfile)
        }
        if ("migrationPolicy" in diff.affectedRootPropertyNames) {
          newSpec = newSpec.copy(migrationPolicy = workingSpec.migrationPolicy)
        }
        if ("resources" in diff.affectedRootPropertyNames) {
          newSpec = newSpec.copy(resources = workingSpec.resources)
        }
        if ("tags" in diff.affectedRootPropertyNames) {
          newSpec = newSpec.copy(tags = workingSpec.tags)
        }
        (overrides as MutableMap)[region] = newSpec
      }
    }

  private suspend fun CloudDriverService.getServerGroups(resource: Resource<TitusClusterSpec>): Iterable<TitusServerGroup> =
    getServerGroups(resource.spec.locations.account,
      resource.spec.moniker,
      resource.spec.locations.regions.map { it.name }.toSet(),
      resource.serviceAccount
    )

  private suspend fun CloudDriverService.getServerGroups(
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
              moniker.name,
              it,
              CLOUD_PROVIDER
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
      // todo eb: how can we tell what version is deployed here?
      // todo: emit an event for the version that's deployed
    }

  private fun TitusActiveServerGroup.toTitusServerGroup() =
    TitusServerGroup(
      name = name,
      location = Location(
        account = placement.account,
        region = region
      ),
      capacity = capacity,
      container = Container(
        organization = image.dockerImageName.split("/").first(),
        image = image.dockerImageName.split("/").last(),
        digest = image.dockerImageDigest
      ),
      entryPoint = entryPoint,
      resources = resources,
      env = env,
      constraints = constraints,
      iamProfile = iamProfile.substringAfterLast("/"),
      capacityGroup = capacityGroup,
      migrationPolicy = migrationPolicy,
      dependencies = ClusterDependencies(
        loadBalancers,
        securityGroupNames = securityGroupNames,
        targetGroups = targetGroups
      )
    )

  private suspend fun getAwsAccountNameForTitusAccount(titusAccount: String): String =
    cloudDriverService.getAccountInformation(titusAccount)["awsAccount"]?.toString()
      ?: throw TitusAccountConfigurationException(titusAccount, "awsAccount")

  private suspend fun getRegistryForTitusAccount(titusAccount: String): String =
    cloudDriverService.getAccountInformation(titusAccount)["registry"]?.toString()
      ?: throw RegistryNotFoundException(titusAccount)

  fun TitusServerGroup.securityGroupIds(): Collection<String> =
    runBlocking {
      val awsAccount = getAwsAccountNameForTitusAccount(location.account)
      dependencies
        .securityGroupNames
        // no need to specify these as Orca will auto-assign them, also the application security group
        // gets auto-created so may not exist yet
        .filter { it !in setOf("nf-infrastructure", "nf-datacenter", moniker.app) }
        .map { cloudDriverCache.securityGroupByName(awsAccount, location.region, it).id }
    }

  private val TitusActiveServerGroup.securityGroupNames: Set<String>
    get() = securityGroups.map {
      cloudDriverCache.securityGroupById(awsAccount, region, it).name
    }
      .toSet()

  private fun Instant.iso() =
    atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_DATE_TIME)

  private fun TitusServerGroup.exportSpec() =
    TitusServerGroupSpec(
      capacity = capacity,
      capacityGroup = capacityGroup,
      constraints = constraints,
      container = container,
      dependencies = dependencies,
      entryPoint = entryPoint,
      env = env,
      iamProfile = iamProfile,
      migrationPolicy = migrationPolicy,
      resources = resources.exportSpec(),
      tags = tags
    )

  private fun Resources.exportSpec() =
    ResourcesSpec(
      cpu = cpu,
      disk = disk,
      gpu = gpu,
      memory = memory,
      networkMbps = networkMbps
    )
}