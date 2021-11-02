package com.netflix.spinnaker.keel.titus.postdeploy

import com.netflix.spinnaker.config.GitLinkConfig
import com.netflix.spinnaker.config.PromoteJarConfig
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.core.api.PromoteJarPostDeployAction
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.titus.ContainerRunner
import com.netflix.spinnaker.keel.titus.deliveryConfigWithClusterAndLoadBalancer
import com.netflix.spinnaker.keel.titus.verification.TASKS
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.hasEntry
import strikt.mockk.captured

class PromoteJarHandlerTests {

  private val context = ArtifactInEnvironmentContext(
    deliveryConfig = deliveryConfigWithClusterAndLoadBalancer(),
    environmentName = "test",
    artifactReference = "fnord",
    version = "1.1"
  )

  private val publishedArtifact = PublishedArtifact(
    type = DOCKER,
    name = "fnord",
    version = "0.161.0-h61.116f116",
    reference = "my.docker.registry/fnord/fnord_0.161.0-h61.116f116",
    metadata = mapOf("buildNumber" to "61", "commitId" to "116f116"),
    provenance = "https://my.jenkins.master/jobs/fnord-release/60",
    buildMetadata = BuildMetadata(
      id = 61,
      number = "61",
      status = "BUILDING",
      uid = "just-a-uid-obviously"
    ),
    gitMetadata = GitMetadata(
      commit = "sha123",
      branch = "main"
    )
  ).normalized()

  private val publisher: EventPublisher = mockk(relaxed = true)
  private val config = PromoteJarConfig(imageId = "promte/image", account = "test", region = "us-west-2")
  private val gitConfig = GitLinkConfig()
  private val keelRepository: KeelRepository = mockk()
  private val containerRunner: ContainerRunner = mockk {
    coEvery { launchContainer(any(), any(), any(), any(), any(), any(), any(), any()) } returns mapOf(TASKS to listOf("1"))
  }

  val subject = PromoteJarHandler(
    publisher,
    config,
    gitConfig,
    keelRepository,
    containerRunner
  )

  @Test
  fun `passes branch to the container`() {
    coEvery { keelRepository.getArtifactVersion(any(), any(), any()) } returns publishedArtifact

    runBlocking { subject.start(context, PromoteJarPostDeployAction()) }

    val containerVars = slot<Map<String, String>>()
    coVerify {
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
        hasEntry("GIT_BRANCH", "main")
    }
  }

  @Test
  fun `defaults to master branch`() {
    coEvery { keelRepository.getArtifactVersion(any(), any(), any()) } returns publishedArtifact.copy(gitMetadata = null)

    runBlocking { subject.start(context, PromoteJarPostDeployAction()) }

    val containerVars = slot<Map<String, String>>()
    coVerify {
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
      hasEntry("GIT_BRANCH", "master")
    }
  }
}
