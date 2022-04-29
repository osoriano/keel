package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.artifacts.ArtifactMetadata
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.Job
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.PullRequest
import com.netflix.spinnaker.keel.api.artifacts.Repo
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.INCREASING_TAG
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.SEMVER_JOB_COMMIT_BY_SEMVER
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.SEMVER_TAG
import com.netflix.spinnaker.keel.api.plugins.SupportedArtifact
import com.netflix.spinnaker.keel.api.support.SpringEventPublisherBridge
<<<<<<< b3859ed7eccc2ab980465dbaacc4d37c0a030215
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
<<<<<<< 9a74071d2401ab3654fed604310d90eb11dd94ff
import com.netflix.spinnaker.keel.clouddriver.model.ArtifactProperty
import com.netflix.spinnaker.keel.clouddriver.model.DockerImage
=======
=======
>>>>>>> ce8ad4d9b3c8a1056ae78ebb313e9d2dbee2719c
import com.netflix.spinnaker.keel.api.artifacts.DockerImage
>>>>>>> d3fd8a09b7e726420d9315438a7435aa9e2a346f
import com.netflix.spinnaker.keel.igor.artifact.ArtifactMetadataService
import com.netflix.spinnaker.keel.test.deliveryConfig
import com.netflix.spinnaker.keel.titus.registry.TitusRegistryService
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isNull
import strikt.assertions.isTrue
import io.mockk.coEvery as every
import io.mockk.coVerify as verify

internal class DockerArtifactSupplierTests : JUnit5Minutests {
  object Fixture {
    val eventBridge: SpringEventPublisherBridge = mockk(relaxUnitFun = true)
    val artifactMetadataService: ArtifactMetadataService = mockk(relaxUnitFun = true)
    val titusRegistryService: TitusRegistryService = mockk()

    val deliveryConfig = deliveryConfig()
    val dockerArtifact = DockerArtifact(
      name = "fnord",
      deliveryConfigName = deliveryConfig.name,
      tagVersionStrategy = SEMVER_TAG
    )
    val versions = listOf("v1.12.1-h1188.35b8b29", "v1.12.2-h1182.8a5b962")
    val artifactProperty = ArtifactProperty(
      metadata = mapOf(
        "labels" to mapOf("purpose" to "test"),
        "registry" to "index.docker.io"
      ),
      name = dockerArtifact.name,
      reference = "index.docker.io/${dockerArtifact.name}:${versions.last()}",
      type = "docker",
      version = "1"
    )

    private val metadata = mapOf(
      "fullImagePath" to artifactProperty.reference,
      "clouddriverAccount" to "test",
      "registry" to "index.docker.io"
    )

    val latestArtifact = PublishedArtifact(
      name = dockerArtifact.name,
      type = dockerArtifact.type,
      reference = dockerArtifact.reference,
      version = versions.last(),
      metadata = metadata
    )

    val latestArtifactWithMetadata = PublishedArtifact(
      name = dockerArtifact.name,
      type = dockerArtifact.type,
      reference = dockerArtifact.reference,
      version = versions.last(),
      metadata = mapOf(
        "buildNumber" to "1",
        "commitId" to "a15p0",
        "prCommitId" to "b26q1",
        "branch" to "master",
        "createdAt" to "1598707355157"
      ) + metadata
    )

    val latestArtifactWithBadVersion = PublishedArtifact(
      name = dockerArtifact.name,
      type = dockerArtifact.type,
      reference = dockerArtifact.reference,
      version = "latest"
    )

    val latestDockerImage = DockerImage(
      account = "test",
      region = "us-east-1",
      repository = latestArtifact.name,
      tag = latestArtifact.version,
      digest = "sha123",
      artifact = artifactProperty,
      registry = "index.docker.io"
    )

    val dockerImageWithMetaData = DockerImage(
      account = "test",
      region = "us-east-1",
      repository = latestArtifact.name,
      tag = latestArtifact.version,
      digest = "sha123",
      commitId = "a15p0",
      prCommitId = "b26q1",
      buildNumber = "1",
      branch = "master",
      date = "1598707355157",
      artifact = artifactProperty,
      registry = "index.docker.io"
    )

    val artifactMetadata = ArtifactMetadata(
      BuildMetadata(
        id = 1,
        uid = "1234",
        startedAt = "yesterday",
        completedAt = "today",
        job = Job(
          name = "job bla bla",
          link = "enkins.com"
        ),
        number = "1"
      ),
      GitMetadata(
        commit = "a15p0",
        author = "keel-user",
        repo = Repo(
          name = "keel",
          link = ""
        ),
        pullRequest = PullRequest(
          number = "111",
          url = "www.github.com/pr/111"
        ),
        commitInfo = Commit(
          sha = "a15p0",
          message = "this is a commit message",
          link = ""
        ),
        project = "spkr",
        branch = "master"
      )
    )

    val dockerArtifactSupplier = DockerArtifactSupplier(eventBridge, artifactMetadataService, titusRegistryService)
  }

  fun tests() = rootContext<Fixture> {
    fixture { Fixture }

    context("DockerArtifactSupplier") {
      val versionSlot = slot<String>()
      before {
        every {
          titusRegistryService.findImages(image = dockerArtifact.name)
        } returns listOf(latestDockerImage)
      }

      test("supports Docker artifacts") {
        expectThat(dockerArtifactSupplier.supportedArtifact).isEqualTo(
          SupportedArtifact(DOCKER, DockerArtifact::class.java)
        )
      }

      test("looks up latest artifact from igor") {
        val result = runBlocking {
          dockerArtifactSupplier.getLatestArtifact(deliveryConfig, dockerArtifact)
        }
        expectThat(result).isEqualTo(latestArtifact)
        verify(exactly = 1) {
          titusRegistryService.findImages(image = latestArtifact.name)
        }
      }

      test("should not process artifact with latest version") {
        expectThat(dockerArtifactSupplier.shouldProcessArtifact(latestArtifact))
          .isTrue()
      }

      test("should not process artifact with latest version") {
        expectThat(dockerArtifactSupplier.shouldProcessArtifact(latestArtifactWithBadVersion))
          .isFalse()
      }
    }

    context("DockerArtifactSupplier with metadata") {
      before {
        every {
          titusRegistryService.findImages(image = dockerArtifact.name)
        } returns listOf(dockerImageWithMetaData)
      }

      test("returns artifact with metadata from Docker image") {
        val results = runBlocking {
          dockerArtifactSupplier.getLatestArtifact(deliveryConfig, dockerArtifact)
        }
        expectThat(results)
          .isEqualTo(latestArtifactWithMetadata)
      }
    }
  }
}
