package com.netflix.spinnaker.keel.orca

import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.keel.actuation.ExecutionSummary
import com.netflix.spinnaker.keel.actuation.RolloutStatus.NOT_STARTED
import com.netflix.spinnaker.keel.actuation.RolloutStatus.RUNNING
import com.netflix.spinnaker.keel.actuation.RolloutStatus.SUCCEEDED
import com.netflix.spinnaker.keel.api.TaskStatus
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
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

    expect {
      that(summary.deployTargets).isNotEmpty().hasSize(2)
      that(summary.deployTargets.map { it.status }.toSet()).containsExactly(SUCCEEDED)
      that(summary.currentStage).isNull()
      that(summary.status).isEqualTo(TaskStatus.SUCCEEDED)
      that(summary.stages).isNotEmpty()
      that(summary.stages[0].startTime).isNotNull().isEqualTo(Instant.parse("2021-10-05T19:46:48.531Z"))
      that(summary.stages[0].endTime).isNotNull().isEqualTo(Instant.parse("2021-10-05T19:50:46.532Z"))
    }
  }

  @Test
  fun `can read running managed rollout stage`() {
    val summary = testSetup("/running-managed-rollout.json")

    expect {
      that(summary.deployTargets).isNotEmpty().hasSize(2)
      that(summary.deployTargets.map { it.status }.toSet()).containsExactlyInAnyOrder(SUCCEEDED, NOT_STARTED)
      that(summary.currentStage).isNotNull().get { type }.isEqualTo("waitForNextRolloutStep")
      that(summary.currentStage?.startTime).isNotNull()
      that(summary.currentStage?.startTime).isEqualTo(Instant.parse("2021-10-05T19:40:18.551Z"))
      that(summary.status).isEqualTo(TaskStatus.RUNNING)
      that(summary.rolloutWorkflowId) isEqualTo "rollout:01FH8ZD7107CZN3V5YKE15RRVA:01FH8ZD710VC49HBF8G9VF7V28"
    }
  }

  @Test
  fun `can read failed managed rollout stage`() {
    val summary = testSetup("/failed-managed-rollout.json")

    expect {
      that(summary.deployTargets).isNotEmpty().hasSize(2)
      that(summary.deployTargets.map { it.status }.toSet()).containsExactlyInAnyOrder(SUCCEEDED, NOT_STARTED)
      that(summary.currentStage).isNull()
      that(summary.status).isEqualTo(TaskStatus.TERMINAL)
      that(summary.rolloutWorkflowId) isEqualTo "rollout:01FH8TGGA62Q8GMWV2149MGW2C:01FH8TGGA6MCFQ1AK3G9F7MZBS"
    }
  }


  @Test
  fun `can read a single region deploy stage`() {
    val summary = testSetup("/single-region-deploy.json")

    expect {
      that(summary.deployTargets).isNotEmpty().hasSize(1)
      that(summary.deployTargets.map { it.status }.toSet()).containsExactly(SUCCEEDED)
      that(summary.currentStage).isNull()
      that(summary.stages).isNotEmpty().hasSize(5)
      that(summary.status).isEqualTo(TaskStatus.SUCCEEDED)
    }
  }

  @Test
  fun `can read a running single region deploy stage`() {
    val summary = testSetup("/running-single-region-deploy.json")

    expect {
      that(summary.deployTargets).isNotEmpty().hasSize(1)
      that(summary.deployTargets.map { it.status }.toSet()).containsExactly(RUNNING)
      that(summary.currentStage).isNotNull().get { type }.isEqualTo("createServerGroup")
      that(summary.stages).isNotEmpty().hasSize(1)
      that(summary.status).isEqualTo(TaskStatus.RUNNING)
    }
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
