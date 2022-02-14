package com.netflix.spinnaker.keel.dgs

import com.netflix.buoy.sdk.model.Location
import com.netflix.buoy.sdk.model.RolloutTarget
import com.netflix.spinnaker.keel.actuation.ExecutionSummary
import com.netflix.spinnaker.keel.actuation.ExecutionSummaryService
import com.netflix.spinnaker.keel.actuation.RolloutStatus.NOT_STARTED
import com.netflix.spinnaker.keel.actuation.RolloutStatus.SUCCEEDED
import com.netflix.spinnaker.keel.actuation.RolloutTargetWithStatus
import com.netflix.spinnaker.keel.api.TaskStatus.RUNNING
import com.netflix.spinnaker.keel.buoy.BuoyClient
import com.netflix.spinnaker.keel.core.api.randomUID
import com.netflix.spinnaker.keel.graphql.types.MD_DeployImmediatelyPayload
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.assertions.isFalse
import strikt.assertions.isSuccess
import strikt.assertions.isTrue
import java.time.Duration
import io.mockk.coVerify as verify

class DeployImmediatelyMutationTests {

  private val executionSummaryService: ExecutionSummaryService = mockk()
  private val buoyClient: BuoyClient = mockk(relaxUnitFun = true)
  private val mutation = DeployImmediatelyMutation(executionSummaryService, buoyClient)

  private val precedingTarget = RolloutTarget("aws", Location("prod", "us-west-2", emptyList()), 0, Duration.ofHours(1))
  private val target = RolloutTarget("aws", Location("prod", "us-east-1", emptyList()), 1, Duration.ofHours(1))
  private val workflowId = randomUID().toString()
  private val execution = ExecutionSummary(
    name = "Some managed rollout task",
    id = randomUID().toString(),
    status = RUNNING,
    stages = emptyList(),
    currentStage = null,
    deployTargets = listOf(
      RolloutTargetWithStatus(
        rolloutTarget = precedingTarget,
        status = SUCCEEDED
      ),
      RolloutTargetWithStatus(
        rolloutTarget = target,
        status = NOT_STARTED
      )
    ),
    rolloutWorkflowId = workflowId
  )
  private val payload = MD_DeployImmediatelyPayload(
    application = "fnord",
    taskId = execution.id,
    region = target.location.region
  )

  private fun invoke() =
    runBlocking {
      mutation.deployImmediately(
        payload = payload,
        user = "fzlem@netflix.com"
      )
    }

  @Test
  fun `fails if the referenced task does not exist`() {
    every { executionSummaryService.getSummary(payload.taskId) } returns null

    expectCatching { invoke() }
      .isSuccess()
      .isFalse()
  }

  @Test
  fun `fails if the referenced task does not have a rollout workflow`() {
    every { executionSummaryService.getSummary(payload.taskId) } returns execution
      .copy(rolloutWorkflowId = null)

    expectCatching { invoke() }
      .isSuccess()
      .isFalse()
  }

  @Test
  fun `fails if the referenced task does not have a target matching the requested region`() {
    every { executionSummaryService.getSummary(payload.taskId) } returns execution
      .copy(deployTargets = execution.deployTargets.filter { it.rolloutTarget != target })

    expectCatching { invoke() }
      .isSuccess()
      .isFalse()
  }

  @Test
  fun `calls Buoy with the workflow id`() {
    every { executionSummaryService.getSummary(payload.taskId) } returns execution

    expectCatching { invoke() }
      .isSuccess()
      .isTrue()

    verify { buoyClient.deployImmediately(workflowId, target) }
  }
}
