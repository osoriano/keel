package com.netflix.spinnaker.keel.verification.container

import com.netflix.spinnaker.config.GitLinkConfig
import com.netflix.spinnaker.keel.api.titus.TitusServerGroup.Location
import com.netflix.spinnaker.keel.titus.ContainerRunner
import com.netflix.spinnaker.keel.titus.TITUS_JOB_TASKS
import com.netflix.spinnaker.keel.verification.BaseVerificationEvaluatorTests
import com.netflix.spinnaker.keel.verification.STANDARD_TEST_PARAMETERS
import com.netflix.spinnaker.keel.verification.StandardTestParameter
import de.huxhorn.sulky.ulid.ULID
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.containsKeys
import strikt.assertions.first
import strikt.assertions.getValue
import strikt.assertions.hasEntry
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isSuccess
import strikt.mockk.captured
import io.mockk.coVerify as verify

internal class TestContainerVerificationEvaluatorTests : BaseVerificationEvaluatorTests() {

  private val app = testContext.deliveryConfig.application

  private val loc = Location(
    account = "titustestvpc",
    region = "us-east-1"
  )

  private val verification = TestContainerVerification(
    image = "illuminati/fnord:latest",
    location = loc,
    application = app
  )

  private val containerRunner: ContainerRunner = mockk()

  override val subject = TestContainerVerificationEvaluator(
    containerRunner = containerRunner,
    linkStrategy = null,
    gitLinkConfig = GitLinkConfig(),
    keelRepository = keelRepository,
    networkEndpointProvider = endpointProvider
  )

  @Test
  fun `starting verification launches a container job via containerRunner`() {
    val taskId = stubTaskLaunch()

    expectCatching { subject.start(testContext, verification) }
      .isSuccess()
      .getValue(TITUS_JOB_TASKS)
      .isA<Iterable<String>>()
      .first() isEqualTo taskId

    val containerVars = slot<Map<String, String>>()

    verify {
      containerRunner.launchContainer(
        imageId = any(),
        description = any(),
        serviceAccount = testContext.deliveryConfig.serviceAccount,
        application = any(),
        environmentName = testContext.environmentName,
        location = verification.location,
        environmentVariables = capture(containerVars),
        containerApplication = any()
      )
    }
  }

  @Test
  fun `standard test parameters are passed into the test container`() {
    stubTaskLaunch()
    runBlocking { subject.start(testContext, verification) }

    val containerVars = slot<Map<String, String>>()
    verify {
      containerRunner.launchContainer(
        imageId = any(),
        description = any(),
        serviceAccount = any(),
        application = any(),
        environmentName = any(),
        location = any(),
        environmentVariables = capture(containerVars),
        containerApplication = any()
      )
    }

    expectThat(containerVars.captured).containsKeys(*STANDARD_TEST_PARAMETERS)
  }

  @Suppress("UNCHECKED_CAST")
  private fun verifyImageId(expectedImageId : String) {
    verify {
      containerRunner.launchContainer(
        imageId = match {
          it == expectedImageId
        },
        description = any(),
        serviceAccount = any(),
        application = any(),
        environmentName = any(),
        location = any(),
        environmentVariables = any(),
        containerApplication = any()
      )
    }
  }

  @Test
  fun `image id specified by image field and tag`() {
    stubTaskLaunch()

    runBlocking {
      subject.start(testContext, TestContainerVerification(image="acme/rollerskates:rocket", location=loc, application=app))
    }

    verifyImageId("acme/rollerskates:rocket")
  }

  @Test
  fun `image id specified by image field, no tag`() {
    stubTaskLaunch()

    runBlocking {
      subject.start(testContext, TestContainerVerification(image="acme/rollerskates", location=loc, application=app))
    }

    verifyImageId("acme/rollerskates:latest")
  }

  @Suppress("UNCHECKED_CAST")
  private fun verifyApplication(expectedApplication : String) {
    verify {
      containerRunner.launchContainer(
        imageId = any(),
        description = any(),
        serviceAccount = any(),
        application = any(),
        environmentName = any(),
        location = any(),
        environmentVariables = any(),
        containerApplication = match {
          it == expectedApplication
        }
      )
    }
  }

  @Test
  fun `container job runs with verification's application`() {
    stubTaskLaunch()
    runBlocking { subject.start(testContext, verification) }
    verifyApplication(verification.application!!)
  }

  @Test
  fun `if no application is specified container job runs with delivery config's application`() {
    stubTaskLaunch()
    runBlocking { subject.start(testContext, verification.copy(application = null)) }
    verifyApplication(testContext.deliveryConfig.application)
  }

  @Test
  fun `environment variables specified in the config are passed to the container`() {
    stubTaskLaunch()

    val envVars = mapOf(
      "SECRET" to "0xACAB",
      "FNORD" to "Zalgo, he comes"
    )
    runBlocking { subject.start(testContext, verification.copy(env = envVars)) }

    val containerVars = slot<Map<String, String>>()
    verify {
      containerRunner.launchContainer(
        imageId = any(),
        description = any(),
        serviceAccount = any(),
        application = any(),
        environmentName = any(),
        location = any(),
        environmentVariables = capture(containerVars),
        containerApplication = any()
      )
    }

    expectThat(containerVars).captured.and {
      envVars.forEach { (key, value) ->
        hasEntry(key, value)
      }
    }
  }

  private fun stubTaskLaunch(): String =
    ULID()
      .nextULID()
      .also { taskId ->
        io.mockk.coEvery {
          containerRunner.launchContainer(
            imageId = any(),
            description = any(),
            serviceAccount = any(),
            application = any(),
            environmentName = any(),
            location = any(),
            environmentVariables = any(),
            containerApplication = any(),
          )
        } answers { mapOf(TITUS_JOB_TASKS to listOf(taskId)) }
      }
}
