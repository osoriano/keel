package com.netflix.spinnaker.keel.api.plugins

import com.netflix.spinnaker.keel.api.ClusterDeployStrategy
import com.netflix.spinnaker.keel.api.ComputeResourceSpec
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.Exportable
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.ResourceDiff
import com.netflix.spinnaker.keel.api.ResourceDiffFactory
import com.netflix.spinnaker.keel.api.RolloutConfig
import com.netflix.spinnaker.keel.api.SimpleLocationProvider
import com.netflix.spinnaker.keel.api.Staggered
import com.netflix.spinnaker.keel.api.actuation.Job
import com.netflix.spinnaker.keel.api.actuation.Task
import com.netflix.spinnaker.keel.api.actuation.TaskLauncher
import com.netflix.spinnaker.keel.core.orcaClusterMoniker
import com.netflix.spinnaker.keel.core.serverGroup
import com.netflix.spinnaker.keel.diff.toIndividualDiffs
import com.netflix.spinnaker.keel.events.ResourceActuationLaunched
import com.netflix.spinnaker.keel.orca.dependsOn
import com.netflix.spinnaker.keel.orca.restrictedExecutionWindow
import com.netflix.spinnaker.keel.orca.waitStage
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import java.time.Clock
import java.time.Duration

/**
 * Common cluster functionality.
 * This class contains abstracted logic where we want all clusters to make the
 * same decision, or abstracted logic involving coordinating resources or
 * publishing events.
 */
abstract class BaseClusterHandler<SPEC: ComputeResourceSpec<*>, RESOLVED: SimpleLocationProvider>(
  resolvers: List<Resolver<*>>,
  protected val taskLauncher: TaskLauncher,
  protected val diffFactory: ResourceDiffFactory,
) : DeployableResourceHandler<SPEC, Map<String, RESOLVED>>, ResolvableResourceHandler<SPEC, Map<String, RESOLVED>>(resolvers) {

  abstract val clock: Clock

  /**
   * parses the desired region from a resource diff
   */
  abstract fun getDesiredRegion(diff: ResourceDiff<RESOLVED>): String

  /**
   * parses the desired account from a resource diff
   */
  abstract fun getDesiredAccount(diff: ResourceDiff<RESOLVED>): String


  abstract fun ResourceDiff<RESOLVED>.moniker(): Moniker

  abstract fun RESOLVED.moniker(): Moniker

  /**
   * returns the entire server group name (including version) from the resolved type.
   *  This is found on the 'name' field in the clouddriver response.
   */
  abstract fun RESOLVED.serverGroup(): String

  /**
   * returns true if the diff is only in whether there are too many clusters enabled
   */
  abstract fun ResourceDiff<RESOLVED>.isEnabledOnly(): Boolean

  /**
   * return true if diff is only in capacity
   */
  abstract fun ResourceDiff<RESOLVED>.isCapacityOnly(): Boolean

  /**
   * return true if diff is only that the server group is disabled
   *
   * This is only relevant for ec2, because disabled server groups have
   *  the scaling processes []Launch, AddToLoadBalancer, Terminate] suspended.
   *  So by default this method returns false to indicate that the diff is
   *  not ignorable.
   */
  open fun ResourceDiff<RESOLVED>.isSuspendPropertiesAndCapacityOnly() = false

  /**
   * return true if diff is only in autoscaling
   */
  abstract fun ResourceDiff<RESOLVED>.isAutoScalingOnly(): Boolean

  /**
   * returns a list of regions where the active server group is unhealthy
   */
  abstract fun getUnhealthyRegionsForActiveServerGroup(resource: Resource<SPEC>): List<String>

  /**
   * Generates a list of possible correlation ids used to check for running executions
   */
  fun generateCorrelationIds(resource: Resource<SPEC>): List<String> =
    resource.regions().map { "${resource.id}:$it" } + "${resource.id}:managed-rollout"

  override suspend fun willTakeAction(
    resource: Resource<SPEC>,
    resourceDiff: ResourceDiff<Map<String, RESOLVED>>
  ): ActionDecision {
    // we can't take any action if there is more than one active server group
    //  AND the current active server group is unhealthy
    val potentialInactionableRegions = mutableListOf<String>()
    val inactionableRegions = mutableListOf<String>()
    diffFactory.toIndividualDiffs(resourceDiff).forEach { diff ->
      if (diff.hasChanges() && diff.isEnabledOnly()) {
        potentialInactionableRegions.add(getDesiredRegion(diff))
      }
    }
    if (potentialInactionableRegions.isNotEmpty()) {
      val unhealthyRegions = getUnhealthyRegionsForActiveServerGroup(resource)
      inactionableRegions.addAll(potentialInactionableRegions.intersect(unhealthyRegions))

    }
    if (inactionableRegions.isNotEmpty()) {
      return ActionDecision(
        willAct = false,
        message = "There is more than one server group enabled " +
          "but the latest is not healthy in ${inactionableRegions.joinToString(" and ")}. " +
          "Spinnaker cannot resolve the problem at this time. " +
          "Manual intervention might be required."
      )
    }
    return ActionDecision(willAct = true)
  }

  override suspend fun delete(resource: Resource<SPEC>): List<Task> {
    val serverGroupsByRegion = getExistingServerGroupsByRegion(resource)
    val regions = serverGroupsByRegion.keys
    val stages = serverGroupsByRegion.flatMap { (region, serverGroups) ->
      serverGroups.map { serverGroup ->
        mapOf(
          "type" to "destroyServerGroup",
          "asgName" to serverGroup.name,
          "moniker" to serverGroup.moniker.orcaClusterMoniker,
          "serverGroupName" to serverGroup.name,
          "region" to region,
          "credentials" to resource.spec.locations.account,
          "cloudProvider" to cloudProvider,
          "user" to resource.serviceAccount,
          // the following 3 properties just say "try all parallel branches but fail the pipeline if any branch fails"
          "completeOtherBranchesThenFail" to true,
          "continuePipeline" to false,
          "failPipeline" to false,
        )
      }
    }
    return if (stages.isEmpty()) {
      emptyList()
    } else {
      listOf(
        taskLauncher.submitJob(
          resource = resource,
          description = "Delete cluster [${resource.name} in account ${resource.spec.locations.account} (${regions.joinToString()})]",
          correlationId = "${resource.id}:delete",
          stages = stages,
          artifactVersion = null
        )
      )
    }
  }

  protected abstract suspend fun getExistingServerGroupsByRegion(resource: Resource<SPEC>): Map<String, List<ServerGroupIdentity>>

  protected abstract val cloudProvider: String

  /**
   * gets current state of the resource and returns the current image, by region.
   */
  abstract suspend fun getImage(resource: Resource<SPEC>): CurrentImages

  /**
   * return a list of stages that resize the server group based on the diff
   */
  abstract fun ResourceDiff<RESOLVED>.resizeServerGroupJob(): Job

  /**
   * return a list of stages that modify the scaling policy based on the diff
   */
  abstract fun ResourceDiff<RESOLVED>.modifyScalingPolicyJob(startingRefId: Int = 0): List<Job>

  /**
   * return the jobs needed to upsert a server group w/o using managed rollout
   */
  abstract fun ResourceDiff<RESOLVED>.upsertOrCloneServerGroupJob(
    resource: Resource<SPEC>,
    startingRefId: Int = 0,
    version: String? = null,
    clone: Boolean = false
  ): Job

  /**
   * returns all the disabled server groups in each region.
   *
   * Used to determine rollback candidates.
   * We only rollback to a disabled server group, so we can ignore the enabled ones.
   */
  abstract suspend fun getDisabledServerGroupsByRegion(resource: Resource<SPEC>): Map<String, List<RESOLVED>>

  abstract fun ResourceDiff<RESOLVED>.rollbackServerGroupJob(resource: Resource<SPEC>, rollbackServerGroup: RESOLVED): Job

  /**
   * return the job needed to upsert a server group using managed rollout
   */
  abstract fun Resource<SPEC>.upsertServerGroupManagedRolloutJob(diffs: List<ResourceDiff<RESOLVED>>, version: String? = null): Job

  /**
   * return the version that will be deployed, represented as an appversion or a tag or a sha
   */
  abstract fun ResourceDiff<RESOLVED>.version(resource: Resource<SPEC>): String

  /**
   * return a list of jobs that disable the oldest server group
   */
  abstract fun ResourceDiff<RESOLVED>.disableOtherServerGroupJob(
    resource: Resource<SPEC>,
    desiredVersion: String
  ): Job

  private fun accountRegionString(diff: ResourceDiff<RESOLVED>): String =
    "${getDesiredAccount(diff)}/${getDesiredRegion(diff)}"

  private fun accountRegionString(resource: Resource<SPEC>, diffs: List<ResourceDiff<RESOLVED>>): String =
    "${resource.account()}/${diffs.map { getDesiredRegion(it) }.joinToString(",")}"

  private fun ResourceDiff<RESOLVED>.capacityOnlyMessage(): String =
    "Resize server group [${moniker()} in ${accountRegionString(this)}]"

  private fun ResourceDiff<RESOLVED>.autoScalingOnlyMessage(): String =
    "Modify auto-scaling [of server group ${moniker()} in ${accountRegionString(this)}]"


  private fun ResourceDiff<RESOLVED>.enabledOnlyMessage(job: Job): String =
    "Disable extra active server group [${job["asgName"]} in ${accountRegionString(this)}]"

  private fun ResourceDiff<RESOLVED>.capacityAndAutoscalingMessage(): String=
    "Modify capacity and auto-scaling [of server group ${moniker()} in ${accountRegionString(this)}]"

  private fun ResourceDiff<RESOLVED>.upsertMessage(version: String): String =
    "Deploy $version [to server group ${moniker()} in ${accountRegionString(this)}]"

  private fun Resource<SPEC>.upsertManagedRolloutMessage(version: String, diffs: List<ResourceDiff<RESOLVED>>): String =
    "Deploy $version [to cluster ${moniker()} in ${accountRegionString(this, diffs)} using a managed rollout]"

  private fun ResourceDiff<RESOLVED>.rollbackMessage(version: String, rollbackServerGroup: RESOLVED) =
    "Rolling back to $version [cluster ${moniker()} in ${accountRegionString(this)} (disabling ${current?.moniker()?.serverGroup}, enabling ${rollbackServerGroup.moniker().serverGroup})]"

  abstract fun correlationId(resource: Resource<SPEC>, diff: ResourceDiff<RESOLVED>): String
  abstract fun Resource<SPEC>.isStaggeredDeploy(): Boolean
  abstract fun Resource<SPEC>.isManagedRollout(): Boolean
  abstract fun Resource<SPEC>.regions(): List<String>
  abstract fun Resource<SPEC>.moniker(): Moniker
  abstract fun Resource<SPEC>.account(): String
  abstract fun ResourceDiff<RESOLVED>.hasScalingPolicies(): Boolean
  abstract fun ResourceDiff<RESOLVED>.isCapacityOrAutoScalingOnly(): Boolean

  /**
   * Checks each region to see if there is a valid server group to roll back to, returns it if so.
   */
  @Suppress("UNCHECKED_CAST")
  suspend fun List<ResourceDiff<RESOLVED>>.getRollbackServerGroupsByRegion(resource: Resource<SPEC>): Map<String, RESOLVED> {
    val disabledServerGroupsByRegion = try {
      getDisabledServerGroupsByRegion(resource)
    } catch (e: Exception) {
      log.error("Error fetching current server groups for rollback for resource ${resource.id}", e)
      // we can ignore any exceptions trying to get the current state here, because
      // rolling back is an optimization.
      emptyMap()
    }

    // check each region for rollback candidates
    return disabledServerGroupsByRegion.mapValues { regionalList ->
      val region = regionalList.key
      val disabledRegionalServerGroups = regionalList.value
      find { regionalDiff ->
        // get the right diff for the region we're checking
        getDesiredRegion(regionalDiff) == region
      }?.let { regionalDiff ->
        // use that diff to see if we have a rollback candidate
        //  (we won't always roll back if we have a rollback candidate.
        //   the upsert logic decides what kind of task to launch to resolve the diff.
        //   this is just providing information)
        disabledRegionalServerGroups.findRollbackCandidate(regionalDiff)
      }
    }.filterValues { it != null } as Map<String, RESOLVED>
  }

  /**
   * Given a regional list of server groups, the resource, and the regional diff,
   * find the first server group that is a rollback candidate (or null if there isn't one)
   */
  private fun List<RESOLVED>.findRollbackCandidate(regionalDiff: ResourceDiff<RESOLVED>): RESOLVED? =
    firstOrNull { candidateServerGroup: RESOLVED ->
      if (candidateServerGroup.serverGroup() == regionalDiff.current?.serverGroup()) {
        // we can't roll back to the same asg, so exclude this from the list of candidates
        false
      } else {
        val candidateDiff = diffFactory.compare(regionalDiff.desired, candidateServerGroup)
        // we can roll back if there's no diff between desired and candidate
        //    (like, if the candidate is a disabled version of the desired)
        // or if the diff is ignorable
        //    (ignorable diffs will be fixed by the rollback task - like capacity differences).
        !candidateDiff.hasChanges() || candidateDiff.isIgnorableForRollback()
      }
    }

  /**
   * returns true if the server group is up and healthy according to its health
   */
  abstract fun isHealthy(serverGroup: RESOLVED, resource: Resource<SPEC>): Boolean

  // rollback task fixes capacity
  private fun ResourceDiff<RESOLVED>.isIgnorableForRollback() =
    isCapacityOnly() || isSuspendPropertiesAndCapacityOnly()

  /**
   * @return `true` if [current] exists and the diff includes a scaling policy change.
   */
  abstract fun ResourceDiff<RESOLVED>.hasScalingPolicyDiff(): Boolean

  /**
   * @return `true` if [current] doesn't exist and desired includes a scaling policy.
   */
  fun ResourceDiff<RESOLVED>.shouldDeployAndModifyScalingPolicies(): Boolean =
    (current == null && hasScalingPolicies()) ||
      (current != null && !isCapacityOrAutoScalingOnly() && hasScalingPolicyDiff())

  abstract fun Resource<SPEC>.getDeployWith(): ClusterDeployStrategy

  /**
   * For titus, a deploying sha can be associated with more than one tag.
   * For ec2, this is irrelevant.
   *
   * override this function if more than one 'version' needs to be marked as deploying
   * when a cluster deploy happens.
   */
  open fun ResourceDiff<RESOLVED>.getDeployingVersions(resource: Resource<SPEC>): Set<String> =
    setOf(version(resource))

  /**
   * consolidates the general orchestration logic to the top level, and delegates the cloud-specific bits
   * to the individual cluster handlers.
   */
  override suspend fun upsert(resource: Resource<SPEC>, resourceDiff: ResourceDiff<Map<String, RESOLVED>>): List<Task> =
    coroutineScope {
      val diffs = diffFactory
        .toIndividualDiffs(resourceDiff)
        .filter { diff -> diff.hasChanges() }

      val deferred: MutableList<Deferred<Task>> = mutableListOf()

      val rollbackServerGroups = diffs.getRollbackServerGroupsByRegion(resource)

      val modifyDiffs = diffs
        .filter {
          it.isCapacityOrAutoScalingOnly() ||
            it.isEnabledOnly() ||
            it.isCapacityOnly() ||
            rollbackServerGroups.containsKey(getDesiredRegion(it))
        }
      val createDiffs = diffs - modifyDiffs

      if (modifyDiffs.isNotEmpty()) {
        deferred.addAll(
          modifyInPlace(resource, modifyDiffs, rollbackServerGroups)
        )
      }

      val version = diffs.first().version(resource)

      if (resource.isStaggeredDeploy() && createDiffs.isNotEmpty()) {
        val tasks = upsertStaggered(resource, createDiffs, version)
        return@coroutineScope tasks + deferred.map { it.await() }
      }

      val launchedTasks = mutableListOf<Task>()

      if (resource.isManagedRollout() && createDiffs.isNotEmpty()) {
        // if managed rollout, do an upsert managed rollout stage.
        val task = upsertManagedRollout(resource, createDiffs, version)
        launchedTasks.add(task)
      } else {
        // otherwise, launch a new task for each region
        deferred.addAll(
          upsertUnstaggered(resource, createDiffs, version)
        )
      }

      if (createDiffs.isNotEmpty()) {
        val versions = createDiffs.map { it.getDeployingVersions(resource) }.flatten().toSet()
        notifyArtifactDeploying(resource, versions)
      }

      return@coroutineScope launchedTasks + deferred.map { it.await() }
    }

  /**
   *  Modifies an existing server group instead of launching a new server group.
   *  This either modifies the enabled server group, or enables a disabled server group.
   */
  private suspend fun modifyInPlace(
    resource: Resource<SPEC>,
    diffs: List<ResourceDiff<RESOLVED>>,
    rollbackServerGroups: Map<String, RESOLVED>
  ): List<Deferred<Task>> =
    coroutineScope {
      diffs.mapNotNull { diff ->
        val rollbackServerGroup = rollbackServerGroups[getDesiredRegion(diff)]
        val (job, description) = when {
          diff.isCapacityOnly() -> listOf(diff.resizeServerGroupJob()) to diff.capacityOnlyMessage()
          diff.isAutoScalingOnly() -> diff.modifyScalingPolicyJob() to diff.autoScalingOnlyMessage()
          diff.isEnabledOnly() -> {
            val appVersion = diff.version(resource)
            val job = diff.disableOtherServerGroupJob(resource, appVersion)
            listOf(job) to diff.enabledOnlyMessage(job)
          }
          rollbackServerGroup != null -> {
            listOf(diff.rollbackServerGroupJob(resource, rollbackServerGroup)) to diff.rollbackMessage(diff.version(resource), rollbackServerGroup)
          }
          else -> listOf(diff.resizeServerGroupJob()) + diff.modifyScalingPolicyJob(1) to diff.capacityAndAutoscalingMessage()
        }

        if (job.isEmpty()) {
          null
        } else {
          log.info("Modifying server group in-place using task: {}", job)

          async {
            taskLauncher.submitJob(
              resource = resource,
              description = description,
              correlationId = correlationId(resource, diff),
              stages = job,
              artifactVersion = diff.version(resource)
            )
          }
        }
      }
    }

  /**
   * Deploys a new server group
   */
  private suspend fun upsertUnstaggered(
    resource: Resource<SPEC>,
    diffs: List<ResourceDiff<RESOLVED>>,
    version: String,
    dependsOn: String? = null
  ): List<Deferred<Task>> =
    coroutineScope {
      diffs.mapNotNull { diff ->
        val stages: MutableList<Map<String, Any?>> = mutableListOf()
        var refId = 0

        if (dependsOn != null) {
          stages.add(dependsOn(dependsOn))
          refId++
        }

        stages.add(diff.upsertOrCloneServerGroupJob(resource, refId, version))
        refId++

        if (diff.shouldDeployAndModifyScalingPolicies()) {
          stages.addAll(diff.modifyScalingPolicyJob(refId))
        }

        if (stages.isEmpty()) {
          null
        } else {
          log.info("Upsert server group using task: {}", stages)

          async {
            taskLauncher.submitJob(
              resource = resource,
              description = diff.upsertMessage(version),
              correlationId = correlationId(resource, diff),
              stages = stages,
              artifactVersion = diff.version(resource)
            )
          }
        }
      }
    }

  private suspend fun upsertStaggered(
    resource: Resource<SPEC>,
    diffs: List<ResourceDiff<RESOLVED>>,
    version: String
  ): List<Task> =
    coroutineScope {
      val regionalDiffs = diffs.associateBy { getDesiredRegion(it) }
      val tasks: MutableList<Task> = mutableListOf()
      var priorExecutionId: String? = null
      val staggeredRegions = resource.getDeployWith().stagger.map {
        it.region
      }
        .toSet()

      // If any, these are deployed in-parallel after all regions with a defined stagger
      val unstaggeredRegions = regionalDiffs.keys - staggeredRegions

      for (stagger in resource.getDeployWith().stagger) {
        if (!regionalDiffs.containsKey(stagger.region)) {
          continue
        }

        val diff = regionalDiffs[stagger.region] as ResourceDiff<RESOLVED>
        val stages: MutableList<Map<String, Any?>> = mutableListOf()
        var refId = 0

        /**
         * Given regions staggered as [A, B, C], this makes the execution of the B
         * `createServerGroup` task dependent on the A task, and C dependent on B,
         * while preserving the unstaggered behavior of an orca task per region.
         */
        if (priorExecutionId != null) {
          stages.add(dependsOn(priorExecutionId))
          refId++
        }

        val stage = diff.upsertOrCloneServerGroupJob(resource, refId).toMutableMap()

        refId++

        /**
         * If regions are staggered by time windows, add a `restrictedExecutionWindow`
         * to the `createServerGroup` stage.
         */
        if (stagger.hours != null) {
          val hours = stagger.hours!!.split("-").map { it.toInt() }
          stage.putAll(restrictedExecutionWindow(hours[0], hours[1]))
        }

        stages.add(stage)

        if (diff.shouldDeployAndModifyScalingPolicies()) {
          stages.addAll(diff.modifyScalingPolicyJob(refId))
        }

        if (stagger.pauseTime != null) {
          stages.add(
            waitStage(stagger.pauseTime!!, stages.size)
          )
        }

        val deferred = async {
          taskLauncher.submitJob(
            resource = resource,
            description = diff.upsertMessage(version),
            correlationId = correlationId(resource, diff),
            stages = stages,
            artifactVersion = diff.version(resource)
          )
        }

        notifyArtifactDeploying(resource, diff.getDeployingVersions(resource))

        val task = deferred.await()
        priorExecutionId = task.id
        tasks.add(task)
      }

      /**
       * `ClusterSpec.stagger` doesn't have to define a stagger for all of the regions clusters.
       * If a cluster deploys into 4 regions [A, B, C, D] but only defines a stagger for [A, B],
       * [C, D] will deploy in parallel after the completion of B and any pauseTime it defines.
       */
      if (unstaggeredRegions.isNotEmpty()) {
        val unstaggeredDiffs = regionalDiffs
          .filter { unstaggeredRegions.contains(it.key) }
          .map { it.value }

        tasks.addAll(
          upsertUnstaggered(resource, unstaggeredDiffs, version, priorExecutionId)
            .map { it.await() }
        )
      }

      return@coroutineScope tasks
    }

  override suspend fun redeploy(deliveryConfig: DeliveryConfig, environment: Environment, resource: Resource<SPEC>) {
    val current = current(resource)
      ?: error("Resource ${resource.id} is not currently deployed and so cannot be redeployed.")

    // use a no-op diff of the current state with itself, so we can leverage the same logic to generate jobs
    val diffs = diffFactory.compare(current, current).let {
      diffFactory.toIndividualDiffs(it)
    }

    coroutineScope {
      diffs.forEach { diff ->
        val version = diff.version(resource)
        val job = diff.upsertOrCloneServerGroupJob(resource, 0, version, clone = true)
        async {
          with(diff.desired) {
            val task = taskLauncher.submitJob(
              resource = resource,
              description = "Redeploy $version [to server group ${resource.spec.moniker} in $account/$region]",
              correlationId = correlationId(resource, diff),
              stages = listOf(job),
              artifactVersion = version
            )
            val redeployingVersions = diff.getDeployingVersions(resource)
            log.debug("Redeploying versions $redeployingVersions for ${resource.spec.moniker} in $account/$region")
            notifyArtifactDeploying(resource, redeployingVersions)
            notifyActuationLaunched(resource, task)
          }
        }
      }
    }
  }

  private suspend fun upsertManagedRollout(
    resource: Resource<SPEC>,
    diffs: List<ResourceDiff<RESOLVED>>,
    version: String
  ): Task {
      val stages = listOf(resource.upsertServerGroupManagedRolloutJob(diffs, version).toMutableMap())
      log.info("Upsert server group using managed rollout and task: {}", stages)

      return taskLauncher.submitJob(
        resource = resource,
        description = resource.upsertManagedRolloutMessage(version, diffs),
        correlationId = "${resource.id}:managed-rollout",
        stages = stages
      )
    }

  private fun notifyArtifactDeploying(resource: Resource<SPEC>, versions: Set<String>) {
    versions.forEach { version ->
      log.debug("Notifying artifact deploying for version $version and resource ${resource.id}")
      notifyArtifactDeploying(resource, version)
    }
  }

  private fun notifyActuationLaunched(resource: Resource<SPEC>, task: Task) =
    eventPublisher?.publishEvent(ResourceActuationLaunched(resource, this@BaseClusterHandler.name, listOf(task), clock))

  // The following overrides are required due to https://youtrack.jetbrains.com/issue/KT-39603

  override suspend fun current(resource: Resource<SPEC>): Map<String, RESOLVED>? =
    TODO("Not yet implemented")

  override suspend fun export(exportable: Exportable): SPEC =
    TODO("Not yet implemented")

  // --- end overrides for KT-39603
}

// todo eb: allow for the list to contain multiple regions for deploying at the same time
fun RolloutConfig.getOrder(region: String): Int {
  val typedStrategy = strategy as? Staggered
  return typedStrategy?.order?.indexOf(region) ?: 0
}

fun RolloutConfig.getPostDeployWait(region: String): Duration {
  val typedStrategy = strategy as? Staggered
  return if (typedStrategy != null) {
    val default = typedStrategy.postDeployWait
    val regionalOverride = typedStrategy.overrides.getOrDefault(region, emptyMap())
    val overrideDuration = regionalOverride["postDeployWait"]?.let { Duration.parse(it.toString()) }
    overrideDuration ?: default ?: Duration.ofMinutes(30)
  } else {
    Duration.ZERO
  }
}
