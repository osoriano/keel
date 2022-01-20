package com.netflix.spinnaker.keel.orca

import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.buoy.sdk.model.Location
import com.netflix.buoy.sdk.model.RolloutTarget
import com.netflix.spinnaker.keel.actuation.ExecutionSummary
import com.netflix.spinnaker.keel.api.TaskStatus
import com.netflix.spinnaker.keel.actuation.RolloutStatus.NOT_STARTED
import com.netflix.spinnaker.keel.actuation.RolloutStatus.RUNNING
import com.netflix.spinnaker.keel.actuation.RolloutStatus.SUCCEEDED
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import okhttp3.ResponseBody
import okhttp3.internal.http.RealResponseBody
import org.junit.jupiter.api.Test
import retrofit2.HttpException
import retrofit2.Response
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEmpty
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import java.time.Instant

class OrcaExecutionSummaryServiceTests {
  val mapper = configuredTestObjectMapper()
  val orcaService: OrcaService = mockk()

  val subject = OrcaExecutionSummaryService(
    orcaService,
    mapper
  )

  @Test
  fun `not found exception returns no summary`() {
    coEvery { orcaService.getOrchestrationExecution(any()) } throws Exception("not found!")
    val summary = subject.getSummary("1")
    expectThat(summary).isNull()
  }

  @Test
  fun `can read managed rollout stage`() {
    val summary = testSetup("/managed-rollout-execution.json")

    expectThat(summary.deployTargets).isNotEmpty().hasSize(2)
    expectThat(summary.deployTargets.map { it.status }.toSet()).containsExactly(SUCCEEDED)
    expectThat(summary.currentStage).isNull()
    expectThat(summary.status).isEqualTo(TaskStatus.SUCCEEDED)
    expectThat(summary.stages).isNotEmpty()
    expectThat(summary.stages[0].startTime).isNotNull().isEqualTo(Instant.parse("2021-10-05T19:46:48Z"))
    expectThat(summary.stages[0].endTime).isNotNull().isEqualTo(Instant.parse("2021-10-05T19:50:46Z"))
  }

  @Test
  fun `can read running managed rollout stage`() {
    val summary = testSetup("/running-managed-rollout.json")

    expectThat(summary.deployTargets).isNotEmpty().hasSize(2)
    expectThat(summary.deployTargets.map { it.status }.toSet()).containsExactlyInAnyOrder(SUCCEEDED, NOT_STARTED)
    expectThat(summary.currentStage).isNotNull().get { type }.isEqualTo("waitForNextRolloutStep")
    expectThat(summary.currentStage?.startTime).isNotNull()
    expectThat(summary.currentStage?.startTime).isEqualTo(Instant.parse("2021-10-05T19:40:18Z"))
    expectThat(summary.status).isEqualTo(TaskStatus.RUNNING)
  }

  @Test
  fun `can read failed managed rollout stage`() {
    val summary = testSetup("/failed-managed-rollout.json")

    expectThat(summary.deployTargets).isNotEmpty().hasSize(2)
    expectThat(summary.deployTargets.map { it.status }.toSet()).containsExactlyInAnyOrder(SUCCEEDED, NOT_STARTED)
    expectThat(summary.currentStage).isNull()
    expectThat(summary.status).isEqualTo(TaskStatus.TERMINAL)
  }


  @Test
  fun `can read a single region deploy stage`() {
    val summary = testSetup("/single-region-deploy.json")

    expectThat(summary.deployTargets).isNotEmpty().hasSize(1)
    expectThat(summary.deployTargets.map { it.status }.toSet()).containsExactly(SUCCEEDED)
    expectThat(summary.currentStage).isNull()
    expectThat(summary.stages).isNotEmpty().hasSize(5)
    expectThat(summary.status).isEqualTo(TaskStatus.SUCCEEDED)
  }

  @Test
  fun `can read a running single region deploy stage`() {
    val summary = testSetup("/running-single-region-deploy.json")

    expectThat(summary.deployTargets).isNotEmpty().hasSize(1)
    expectThat(summary.deployTargets.map { it.status }.toSet()).containsExactly(RUNNING)
    expectThat(summary.currentStage).isNotNull().get { type }.isEqualTo("createServerGroup")
    expectThat(summary.stages).isNotEmpty().hasSize(1)
    expectThat(summary.status).isEqualTo(TaskStatus.RUNNING)
  }

  @Test
  fun `disableServerGroup`(){
    val summary = testSetup("/disable-server-group.json")
    expectThat(summary.deployTargets).isNotEmpty().hasSize(1)
    val target = summary.deployTargets.first().rolloutTarget
    expect {
      that(target.cloudProvider).isEqualTo("titus")
      that(target.location.account).isEqualTo("titustestvpc")
      that(target.location.region).isEqualTo("us-east-1")
    }
  }

  @Test
  fun `resizeServerGroup`(){
    val summary = testSetup("/resize-server-group.json")
    expectThat(summary.deployTargets).isNotEmpty().hasSize(1)
    val target = summary.deployTargets.first().rolloutTarget
    expect {
      that(target.cloudProvider).isEqualTo("titus")
      that(target.location.account).isEqualTo("titustestvpc")
      that(target.location.region).isEqualTo("us-east-1")
    }
  }

  @Test
  fun `rollbackServerGroup`(){
    val summary = testSetup("/rollback-server-group.json")
    expectThat(summary.deployTargets).isNotEmpty().hasSize(1)
    val target = summary.deployTargets.first().rolloutTarget
    expect {
      that(target.cloudProvider).isEqualTo("aws")
      that(target.location.account).isEqualTo("mgmt")
      that(target.location.region).isEqualTo("us-west-2")
    }
  }

  @Test
  fun `load balancer`(){
    val summary = testSetup("/upsert-load-balancer-stage.json")
    expectThat(summary.deployTargets).isNotEmpty().hasSize(1)
    val target = summary.deployTargets.first().rolloutTarget
    expect {
      that(target.cloudProvider).isEqualTo("aws")
      that(target.location.account).isEqualTo("mgmttest")
      that(target.location.region).isEqualTo("us-west-2")
      that(target.location.sublocations).containsExactlyInAnyOrder("us-west-2a", "us-west-2b", "us-west-2c")
    }
  }

  @Test
  fun `security group`(){
    val summary = testSetup("/upsert-security-group-stage.json")
    expectThat(summary.deployTargets).isNotEmpty().hasSize(1)
    val target = summary.deployTargets.first().rolloutTarget
    expect {
      that(target.cloudProvider).isEqualTo("aws")
      that(target.location.account).isEqualTo("mgmttest")
      that(target.location.region).isEqualTo("us-west-2")
    }
  }

  @Test
  fun `scaling policies`(){
    val summary = testSetup("/modify-scaling-policies.json")
    expectThat(summary.deployTargets).isNotEmpty().hasSize(1)
    val target = summary.deployTargets.first().rolloutTarget
    expect {
      that(target.cloudProvider).isEqualTo("aws")
      that(target.location.account).isEqualTo("prod")
      that(target.location.region).isEqualTo("us-east-1")
    }
  }


  @Test
  fun `redeploy`(){
    val summary = testSetup("/redeploy-execution.json")
    expectThat(summary.deployTargets).isNotEmpty().hasSize(1)
    val target = summary.deployTargets.first().rolloutTarget
    expect {
      that(target.cloudProvider).isEqualTo("aws")
      that(target.location.account).isEqualTo("mgmt")
      that(target.location.region).isEqualTo("us-west-2")
    }
  }

  @Test
  fun `dgs task has no targets`(){
    val summary = testSetup("/dgs-deploy.json")
    expectThat(summary.deployTargets).isEmpty()
    expectThat(summary.status).isEqualTo(TaskStatus.SUCCEEDED)
  }

  @Suppress("RECEIVER_NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
  fun testSetup(fileName: String): ExecutionSummary {
    val response = javaClass.getResource(fileName).readText()
    coEvery { orcaService.getOrchestrationExecution(any()) } returns mapper.readValue(response)

    return runBlocking {
      subject.getSummary("1")!!
    }
  }
}
