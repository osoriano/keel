package com.netflix.spinnaker.keel.verification.jenkins

import com.netflix.spinnaker.config.GitLinkConfig
import com.netflix.spinnaker.keel.actuation.Stage
import com.netflix.spinnaker.keel.api.TaskStatus
import com.netflix.spinnaker.keel.api.TaskStatus.NOT_STARTED
import com.netflix.spinnaker.keel.api.TaskStatus.RUNNING
import com.netflix.spinnaker.keel.api.TaskStatus.SUCCEEDED
import com.netflix.spinnaker.keel.api.action.ActionState
import com.netflix.spinnaker.keel.api.actuation.SubjectType.VERIFICATION
import com.netflix.spinnaker.keel.api.actuation.Task
import com.netflix.spinnaker.keel.api.actuation.TaskLauncher
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.FAIL
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.NOT_EVALUATED
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PASS
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PENDING
import com.netflix.spinnaker.keel.model.OrcaJob
import com.netflix.spinnaker.keel.orca.ExecutionDetailResponse
import com.netflix.spinnaker.keel.orca.OrcaExecutionStages
import com.netflix.spinnaker.keel.verification.BaseVerificationEvaluatorTests
import com.netflix.spinnaker.keel.verification.STANDARD_TEST_PARAMETERS
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_COMMIT_SHA
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_EUREKA_CLUSTER
import de.huxhorn.sulky.ulid.ULID
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.containsKeys
import strikt.assertions.first
import strikt.assertions.getValue
import strikt.assertions.hasEntry
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isSuccess
import java.time.Instant
import io.mockk.coEvery as every
import io.mockk.coVerify as verify

internal class JenkinsJobVerificationEvaluatorTests : BaseVerificationEvaluatorTests() {
  private val verification = JenkinsJobVerification(
    controller = "krypton",
    job = "USERS-alice-wonderland-integration-test"
  )

  private val taskLauncher: TaskLauncher = mockk()
  private val gitLinkConfig = GitLinkConfig()

  override val subject = JenkinsJobVerificationEvaluator(
    taskLauncher = taskLauncher,
    keelRepository = keelRepository,
    gitLinkConfig = gitLinkConfig,
    networkEndpointProvider = endpointProvider
  )

  @Test
  fun `starting verification launches a jenkins task`() {
    val taskId = stubTaskLaunch()

    expectCatching { subject.start(testContext, verification) }
      .isSuccess()
      .getValue("tasks")
      .isA<List<String>>()
      .first().isEqualTo(taskId)

    val stages = slot<List<OrcaJob>>()

    verify {
      with(testContext) {
        taskLauncher.submitJob(
          type = VERIFICATION,
          environmentName = environmentName,
          resourceId = null,
          description = any(),
          user = deliveryConfig.serviceAccount,
          application = deliveryConfig.application,
          notifications = emptySet(),
          stages = capture(stages)
        )
      }
    }

    expectThat(stages.captured.first())
      .isA<Map<String, Any>>()
      .getValue("type").isEqualTo("jenkins")
  }

  @Test
  fun `standard test parameters are passed to the Jenkins job`() {
    stubTaskLaunch()
    runBlocking { subject.start(testContext, verification) }

    val job = captureJob()
    expectThat(job["parameters"])
      .isA<Map<String, Any>>()
      .containsKeys(*STANDARD_TEST_PARAMETERS)
  }

  @Test
  fun `static parameters specified in the config are passed to the Jenkins job`() {
    stubTaskLaunch()

    val staticParams = mapOf(
      "SECRET" to "0xACAB",
      "FNORD" to "Zalgo, he comes"
    )

    runBlocking {
      subject.start(testContext, verification.copy(staticParameters = staticParams))
    }

    val job = captureJob()

    expectThat(job["parameters"])
      .isA<Map<String, Any>>()
      .and {
        staticParams.forEach { (key, value) ->
          hasEntry(key, value)
        }
      }
  }

  @Test
  fun `dynamic parameters specified in the config are interpreted and passed to the Jenkins job`() {
    stubTaskLaunch()

    val dynamicParams = mapOf(
      "CLUSTER" to TEST_EUREKA_CLUSTER,
      "COMMIT" to TEST_COMMIT_SHA
    )

    runBlocking {
      subject.start(testContext, verification.copy(dynamicParameters = dynamicParams))
    }

    val job = captureJob()

    expectThat(job["parameters"])
      .isA<Map<String, Any>>()
      .and {
        getValue("CLUSTER").isEqualTo(eurekaClusterDns)
        getValue("COMMIT").isEqualTo(publishedArtifact.commitHash)
      }
  }

  @ParameterizedTest
  @EnumSource(value = TaskStatus::class)
  fun `verification state reflects jenkins task status`(taskStatus: TaskStatus) {
    val startTime = Instant.now()
    val startState = ActionState(NOT_EVALUATED, startedAt = startTime, endedAt = null, metadata = mapOf("tasks" to listOf("fake")))

    every {
      taskLauncher.getTaskExecution(any())
    } answers {
      ExecutionDetailResponse(
        id = arg(0),
        name = "fake",
        application = testContext.deliveryConfig.application,
        buildTime = startTime,
        startTime = startTime,
        endTime = null,
        status = taskStatus
      )
    }

    val actionState = runBlocking {
      subject.evaluate(testContext, verification, startState)
    }

    expectThat(actionState.status) {
      when {
        taskStatus.isSuccess() -> isEqualTo(PASS)
        taskStatus.isIncomplete() -> isEqualTo(PENDING)
        else -> isEqualTo(FAIL)
      }
    }
  }

  @Test
  fun `verification state includes jenkins job link`() {
    val startTime = Instant.now()
    val startState = ActionState(NOT_EVALUATED, startedAt = startTime, endedAt = null, metadata = mapOf("tasks" to listOf("fake")))

    every {
      taskLauncher.getTaskExecution(any())
    } answers {
      ExecutionDetailResponse(
        id = arg(0),
        name = "fake",
        application = testContext.deliveryConfig.application,
        buildTime = startTime,
        startTime = startTime,
        endTime = null,
        status = RUNNING,
        execution = OrcaExecutionStages(
          stages = listOf(
            Stage(
              id = "id",
              type = "jenkins",
              name = "jenkins",
              status = RUNNING,
              context = mapOf(
                "buildInfo" to mapOf(
                  "url" to "https://jenkins/job/123/"
                )
              )
            )
          )
        )
      )
    }

    val actionState = runBlocking {
      subject.evaluate(testContext, verification, startState)
    }

    expectThat(actionState.link).isEqualTo("https://jenkins/job/123/")
  }

  private fun captureJob(): OrcaJob {
    val stages = slot<List<OrcaJob>>()

    verify {
      taskLauncher.submitJob(
        type = VERIFICATION,
        environmentName = any(),
        resourceId = null,
        description = any(),
        user = any(),
        application = any(),
        notifications = any(),
        stages = capture(stages)
      )
    }

    return stages.captured.first()
  }

  private fun stubTaskLaunch(): String =
    ULID().nextULID().also { taskId ->
      every {
        taskLauncher.submitJob(
          type = VERIFICATION,
          environmentName = any(),
          resourceId = any(),
          description = any(),
          user = any(),
          application = any(),
          notifications = any(),
          stages = any()
        )
      } returns Task(taskId, "fake")
    }
}
