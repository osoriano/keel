package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsMutation
import com.netflix.graphql.dgs.InputArgument
import com.netflix.spinnaker.keel.actuation.ExecutionSummaryService
import com.netflix.spinnaker.keel.actuation.RolloutTargetWithStatus
import com.netflix.spinnaker.keel.buoy.BuoyClient
import com.netflix.spinnaker.keel.graphql.DgsConstants.MUTATION.Md_deployResourceImmediately
import com.netflix.spinnaker.keel.graphql.types.MD_DeployResourceImmediatelyPayload
import org.slf4j.LoggerFactory
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.RequestHeader

@DgsComponent
class DeployImmediatelyMutation(
  private val executionSummaryService: ExecutionSummaryService,
  private val buoyClient: BuoyClient
) {
  /**
   * DGS mutation that triggers a managed rollout to start deploying a region immediately.
   *
   * Fails if:
   * - no task with an id matching the one in [payload] is found.
   * - the task does not have a rollout workflow.
   * - the task doesn't have a target with a region that matches the one in [payload].
   */
  @DgsMutation(field = Md_deployResourceImmediately)
  @PreAuthorize("@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)")
  suspend fun deployImmediately(
    @InputArgument payload: MD_DeployResourceImmediatelyPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean =
    runCatching {
      val task = checkNotNull(executionSummaryService.getSummary(payload.taskId)) {
        "Execution ${payload.taskId} does not exist"
      }
      val workflowId = checkNotNull(task.rolloutWorkflowId) {
        "Execution ${payload.taskId} does not contain a workflow id for a rollout"
      }
      val targets = payload.regions.map { checkNotNull(task.deployTargets.forRegion(it)) }
      log.debug("Deploying the following regions of workflowId $workflowId for app ${payload.application} - ${payload.regions}")
      buoyClient.deployRegionsImmediately(workflowId, targets)
      true
    }
      .getOrElse { err ->
        log.error("Error attempting to deploy immediately", err)
        false
      }

  private fun Iterable<RolloutTargetWithStatus>.forRegion(region: String) =
    firstOrNull { it.rolloutTarget.location.region == region }?.rolloutTarget

  private val log by lazy { LoggerFactory.getLogger(javaClass) }
}
