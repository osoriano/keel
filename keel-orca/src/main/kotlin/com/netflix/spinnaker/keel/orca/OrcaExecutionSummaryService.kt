package com.netflix.spinnaker.keel.orca

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.buoy.sdk.model.Location as BuoyLocation
import com.netflix.buoy.sdk.model.RolloutStep
import com.netflix.buoy.sdk.model.RolloutTarget
import com.netflix.spinnaker.keel.actuation.ExecutionSummary
import com.netflix.spinnaker.keel.actuation.ExecutionSummaryService
import com.netflix.spinnaker.keel.actuation.RolloutStatus
import com.netflix.spinnaker.keel.actuation.RolloutTargetWithStatus
import com.netflix.spinnaker.keel.actuation.Stage
import com.netflix.spinnaker.keel.api.TaskStatus.CANCELED
import com.netflix.spinnaker.keel.api.TaskStatus.FAILED_CONTINUE
import com.netflix.spinnaker.keel.api.TaskStatus.RUNNING
import com.netflix.spinnaker.keel.api.TaskStatus.SKIPPED
import com.netflix.spinnaker.keel.api.TaskStatus.STOPPED
import com.netflix.spinnaker.keel.api.TaskStatus.TERMINAL
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Service for translating a task into a nice summary
 */
@Suppress("UNCHECKED_CAST")
@Component
class OrcaExecutionSummaryService(
  private val orcaService: OrcaService,
  private val mapper: ObjectMapper,
) : ExecutionSummaryService {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  companion object {
    val COMPLETED_TARGETS_STAGE = "initManagedRolloutStep"
    val KICKOFF_STAGE = "startManagedRollout"
    val DEPLOY_STAGE = "deploy"

    val SECURITY_GROUP_STAGE = "upsertSecurityGroup"
    val LOAD_BALANCER_STAGE = "upsertLoadBalancer"
    val MANAGED_ROLLOUT_STAGE = "managedRollout"
    val RESIZE_SERVER_GROUP_STAGE = "resizeServerGroup"
    val ROLLBACK_SERVER_GROUP_STAGE = "rollbackServerGroup"
    val DISABLE_SERVER_GROUP_STAGE = "disableServerGroup"
    val CREATE_SERVER_GROUP_STAGE = "createServerGroup"
    val MODIFY_SCALING_POLICY_STAGE = "upsertScalingPolicy"
    val CLONE_SERVER_GROUP_STAGE = "cloneServerGroup"
    val PUBLISH_DGS_STAGE = "dgsSchemaDeploy"
  }

  override fun getSummary(executionId: String): ExecutionSummary {
    val taskDetails = runBlocking {
      orcaService.getOrchestrationExecution(executionId)
    }

    val typedStages: List<OrcaStage> =
      taskDetails.execution?.stages?.map { stage -> stage.mapValues {
        (key, value) ->
        if ((key == "startTime" || key == "endTime") && value is Long) {
          value / 1000 // orca returns the value in ms, so we need to convert it to seconds
        } else {
          value
        }
      } }?.map { mapper.convertValue(it) }
        ?: emptyList()

    val currentStage = typedStages
      .filter { it.status == RUNNING }
      .maxByOrNull { it.refId?.length ?: 0 } //grab the longest ref id, which will be the most nested running stage

    val targets = getTargets(taskDetails, typedStages)

    return ExecutionSummary(
      name = taskDetails.name,
      id = taskDetails.id,
      status = taskDetails.status,
      currentStage = currentStage?.toStage(),
      stages = typedStages.map { it.toStage() },
      deployTargets = targets,
      error = if (taskDetails.status.isFailure()) taskDetails.execution?.stages.getFailureMessage(mapper) else null
    )
  }

  /**
   * Create the rollout targets and determine their status
   */
  fun getTargets(execution: ExecutionDetailResponse, typedStages: List<OrcaStage>): List<RolloutTargetWithStatus> {
    val targetsWithStatus: MutableList<RolloutTargetWithStatus> = mutableListOf()
    val statusTargetMap = when {
      execution.isManagedRollout() -> getTargetStatusManagedRollout(typedStages)
      execution.isDGS() -> emptyMap() // dgs tasks don't have regional targets
      else -> getTargetStatus(execution, typedStages)
    }

    statusTargetMap.forEach { (status, targets) ->
      targetsWithStatus.addAll(
        targets.map {
          RolloutTargetWithStatus(
            rolloutTarget = it,
            status = status
          )
        }
      )
    }

    return targetsWithStatus
  }

  fun ExecutionDetailResponse.isManagedRollout(): Boolean =
    containsStageType(MANAGED_ROLLOUT_STAGE)

  fun ExecutionDetailResponse.isDeployServerGroup(): Boolean =
    containsStageType(CREATE_SERVER_GROUP_STAGE) && !containsStageType(MANAGED_ROLLOUT_STAGE)

  fun ExecutionDetailResponse.isUpsertSecurityGroup(): Boolean =
    containsStageType(SECURITY_GROUP_STAGE)

  fun ExecutionDetailResponse.isUpsertLoadBalancer(): Boolean =
    containsStageType(LOAD_BALANCER_STAGE)

  fun ExecutionDetailResponse.isResizeServerGroup(): Boolean =
    containsStageType(RESIZE_SERVER_GROUP_STAGE)

  fun ExecutionDetailResponse.isRollbackServerGroup(): Boolean =
    containsStageType(ROLLBACK_SERVER_GROUP_STAGE)

  fun ExecutionDetailResponse.isDisableServerGroup(): Boolean =
    containsStageType(DISABLE_SERVER_GROUP_STAGE)

  fun ExecutionDetailResponse.isModifyScalingPolicies(): Boolean =
    containsStageType(MODIFY_SCALING_POLICY_STAGE)

  fun ExecutionDetailResponse.isRedploy(): Boolean =
    containsStageType(CLONE_SERVER_GROUP_STAGE)

  fun ExecutionDetailResponse.isDGS(): Boolean =
    containsStageType(PUBLISH_DGS_STAGE)

  fun ExecutionDetailResponse.containsStageType(type: String): Boolean =
    execution?.stages?.find { it["type"] == type } != null

  fun getTargetStatusManagedRollout(
    typedStages: List<OrcaStage>
  ): Map<RolloutStatus, List<RolloutTarget>> {
    val targets: MutableMap<RolloutStatus, List<RolloutTarget>> = mutableMapOf()

    // completed targets will be listed in the outputs of this type of stage
    targets[RolloutStatus.SUCCEEDED] = typedStages
      .filter { it.type == COMPLETED_TARGETS_STAGE }
      .mapNotNull { it.outputs["completedRolloutStep"] }
      .map<Any, RolloutStep> { mapper.convertValue(it) }
      .flatMap { it.targets }

    // deploying targets will be listed in the context of the deploy stage,
    // so we filter for running deploy stages
    targets[RolloutStatus.RUNNING] = typedStages
      .filter {
        it.type == DEPLOY_STAGE &&
          it.status == RUNNING
      }
      .map { stage ->
        val runningTargets = stage.context["targets"] as? List<Map<*, *>> ?: emptyList()
        mapper.convertValue<List<RolloutTarget>>(runningTargets)
      }
      .flatten()

    targets[RolloutStatus.FAILED] = typedStages
      .filter {
        it.type == DEPLOY_STAGE &&
          listOf(FAILED_CONTINUE, TERMINAL, CANCELED, SKIPPED, STOPPED).contains(it.status)
      }
      .map { stage ->
        val runningTargets = stage.context["targets"] as? List<Map<*, *>> ?: emptyList()
        mapper.convertValue(runningTargets)
      }

    targets[RolloutStatus.NOT_STARTED] = (typedStages
      .firstOrNull {
        it.type == KICKOFF_STAGE
      }
      ?.let { stage ->
        val allTargets = stage.context["targets"] as? List<Map<*, *>> ?: emptyList()
        mapper.convertValue<List<RolloutTarget>>(allTargets)
      } ?: emptyList())
      .filter { target ->
        target.notIn(targets[RolloutStatus.SUCCEEDED] as List<RolloutTarget>) &&
          target.notIn(targets[RolloutStatus.FAILED] as List<RolloutTarget>) &&
          target.notIn(targets[RolloutStatus.RUNNING] as List<RolloutTarget>)
      }

    return targets
  }

  /**
   * Normal equals/comparison doesn't work when we use the java objects, so I must
   * write my own.
   */
  fun RolloutTarget.notIn(targets: List<RolloutTarget>): Boolean {
    targets.forEach { target ->
      if (target.cloudProvider == cloudProvider &&
        target.location.region == location.region &&
        target.location.account == location.account &&
        target.location.sublocations == location.sublocations
      ) {
        return false
      }
    }
    return true
  }

  fun getTargetStatus(
    execution: ExecutionDetailResponse,
    typedStages: List<OrcaStage>
  ): Map<RolloutStatus, List<RolloutTarget>> {
    val targets = when {
      execution.isDeployServerGroup() -> {
        val deployStage = typedStages.firstOrNull { it.type == CREATE_SERVER_GROUP_STAGE }
        createTargetFromDeployStage(deployStage)
      }
      execution.isUpsertLoadBalancer() -> {
        val upsertLoadBalancerStage = typedStages.firstOrNull { it.type == LOAD_BALANCER_STAGE }
        createTargetFromLoadBalancer(upsertLoadBalancerStage)
      }
      execution.isUpsertSecurityGroup() -> {
        val upsertSecurityGroupStage = typedStages.firstOrNull { it.type == SECURITY_GROUP_STAGE }
        createTargetFromSecurityGroup(upsertSecurityGroupStage)
      }
      execution.isResizeServerGroup() -> {
        typedStages.firstOrNull { it.type == RESIZE_SERVER_GROUP_STAGE }?.let { stage ->
          generateTargetForRegion(stage)
        }
      }
      execution.isRollbackServerGroup() -> {
        typedStages.firstOrNull { it.type == ROLLBACK_SERVER_GROUP_STAGE }?.let { stage ->
          generateTargetForRegion(stage)
        }
      }
      execution.isDisableServerGroup() -> {
        typedStages.firstOrNull { it.type == DISABLE_SERVER_GROUP_STAGE }?.let { stage ->
          generateTargetForRegion(stage)
        }
      }
      execution.isModifyScalingPolicies() -> {
        typedStages.firstOrNull { it.type == MODIFY_SCALING_POLICY_STAGE }?.let { stage ->
          generateTargetForRegion(stage)
        }
      }
      execution.isRedploy() -> {
        typedStages.firstOrNull { it.type == CLONE_SERVER_GROUP_STAGE }?.let { stage ->
          generateTargetFromSource(stage)
        }
      }
      else -> {
        log.error("Unknown task type for task ${execution.id}, attempting generic parsing.")
        // attempt some parsing if we don't know the stage type
        // find the parent stage - usually refId of 0 or 1, we are not consistent in how we construct it
        val stage = typedStages.find { it.refId == "0" } ?: typedStages.find { it.refId == "1" }
        stage?.let {
          // try the basic parsing
          generateTargetForRegion(stage)
        }
      }
    } ?: emptyList()

    return addExecutionStatusToTargets(targets, execution)
  }

  /**
   * For targets that are deployed using a single task per region
   *   the target will have the same status as the execution.
   *   This function calculates that status.
   */
  fun addExecutionStatusToTargets(
    targets: List<RolloutTarget>,
    execution: ExecutionDetailResponse
  ): Map<RolloutStatus, List<RolloutTarget>> =
    when {
      execution.status.isSuccess() -> mapOf(RolloutStatus.SUCCEEDED to targets)
      execution.status.isFailure() -> mapOf(RolloutStatus.FAILED to targets)
      execution.status.isIncomplete() -> mapOf(RolloutStatus.RUNNING to targets)
      else -> mapOf(RolloutStatus.NOT_STARTED to targets)
    }

  fun createTargetFromDeployStage(deployStage: OrcaStage?): List<RolloutTarget> {
    if (deployStage == null || deployStage.type != CREATE_SERVER_GROUP_STAGE) {
      return emptyList()
    }

    var account: String = deployStage.getAccount() ?: "unable to find account"
    val cloudProvider: String = deployStage.getCloudProvider() ?: "unable to find cloud provider"
    val serverGroups = deployStage.context["deploy.server.groups"] as? Map<*, *>
    val regions: MutableSet<String> = serverGroups?.keys as? MutableSet<String> ?: mutableSetOf()

    if (regions.isEmpty()) {
      // deploy stage is hasn't created the server group yet,
      // let's see if we can find the info from the source server group
      val source = deployStage.context["source"] as? Map<*, *> ?: emptyMap<String, String>()
      source["region"]?.let { regions.add(it as String) }
      source["account"]?.let { account = it as String }
    }

    return regions.map { region ->
      RolloutTarget(
        cloudProvider,
        BuoyLocation(account, region, emptyList()),
        null,
        null
      )
    }
  }

  fun createTargetFromSecurityGroup(upsertSecurityGroupStage: OrcaStage?): List<RolloutTarget> {
    if (upsertSecurityGroupStage == null || upsertSecurityGroupStage.type != SECURITY_GROUP_STAGE) {
      return emptyList()
    }
    val regions = upsertSecurityGroupStage.getRegions()
    val account = upsertSecurityGroupStage.getAccount() ?: "unable to find account"
    val cloudProvider: String = upsertSecurityGroupStage.getCloudProvider() ?: "unable to find cloud provider"
    return regions.map { region ->
      RolloutTarget(
        cloudProvider,
        BuoyLocation(account, region, emptyList()),
        null,
        null
      )
    }
  }

  /**
   * Most single region server groups stages have a similar form:
   *   account, cloud provider, and region are all in the context of the stage with the same key.
   * This function takes that general form and creates a rollback target from it.
   */
  private fun generateTargetForRegion(stage: OrcaStage): List<RolloutTarget> {
    val region: String = stage.getRegion() ?: "unable to find region"
    val account = stage.getAccount() ?: "unable to find account"
    val cloudProvider: String = stage.getCloudProvider() ?: "unable to find cloud provider"
    return listOf(
      RolloutTarget(
        cloudProvider,
        BuoyLocation(account, region, emptyList()),
        null,
        null
      )
    )
  }

  /**
   * Clone stages have target info in the "source" key in the context block, like:
   *  source": {
   *    "asgName": "orca-prestaging-v494",
   *    "region": "us-west-2",
   *    "account": "mgmt"
   *  },
  */
  private fun generateTargetFromSource(stage: OrcaStage): List<RolloutTarget> {
    val source = stage.context["source"] as? Map<String, String> ?: emptyMap()
    val region = source["region"] ?: "unable to find region"
    val account = stage.getAccount() ?: "unable to find account"
    val cloudProvider: String = stage.getCloudProvider() ?: "unable to find cloud provider"
    return listOf(
      RolloutTarget(
        cloudProvider,
        BuoyLocation(account, region, emptyList()),
        null,
        null
      )
    )
  }

  data class LoadBalancerOrcaTarget(
    val credentials: String,
    val vpcId: String,
    val name: String,
    val availabilityZones: Map<String, List<String>> //key is region, value is list of azs
  )

  fun createTargetFromLoadBalancer(upsertLoadBalancerStage: OrcaStage?): List<RolloutTarget> {
    if (upsertLoadBalancerStage == null || upsertLoadBalancerStage.type != LOAD_BALANCER_STAGE) {
      return emptyList()
    }

    val cloudProvider: String = upsertLoadBalancerStage.getCloudProvider() ?: "unable to find cloud provider"
    val orcaTargets: List<LoadBalancerOrcaTarget> = mapper.convertValue(upsertLoadBalancerStage.context["targets"] ?: emptyList<LoadBalancerOrcaTarget>())

    return orcaTargets.map { target ->
      val rolloutTargets = mutableListOf<RolloutTarget>()
      target.availabilityZones.forEach { (region, zones) ->
        rolloutTargets.add(
          RolloutTarget(
            cloudProvider,
            BuoyLocation(target.credentials, region , zones),
            null,
            null
          )
        )
      }
      rolloutTargets
    }.flatten()
  }

  fun OrcaStage.getAccount(): String? =
    context["credentials"] as? String

  fun OrcaStage.getCloudProvider(): String? =
    context["cloudProvider"] as? String

  fun OrcaStage.getRegion(): String? =
    context["region"] as? String

  fun OrcaStage.getRegions(): List<String> =
    context["regions"] as? List<String> ?: emptyList()

  fun OrcaStage.toStage() =
    Stage(
      id = id,
      type = type,
      name = name,
      startTime = startTime,
      endTime = endTime,
      status = status,
      refId = refId,
      requisiteStageRefIds = requisiteStageRefIds,
    )
}
