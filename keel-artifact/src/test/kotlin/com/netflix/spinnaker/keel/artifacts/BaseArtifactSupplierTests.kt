package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.artifacts.ArtifactMetadata
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.Job
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.PullRequest
import com.netflix.spinnaker.keel.api.artifacts.Repo
import com.netflix.spinnaker.keel.api.plugins.SupportedArtifact
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.igor.artifact.ArtifactMetadataService
import com.netflix.spinnaker.keel.test.DummyArtifact
import com.netflix.spinnaker.keel.test.DummySortingStrategy
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class BaseArtifactSupplierTests {
  private val commitHash = "a15p0"
  private val artifactMetadata = ArtifactMetadata(
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
      commit = commitHash,
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
        sha = commitHash,
        message = "this is a commit message",
        link = ""
      ),
      project = "spkr",
      branch = "master"
    )
  )

  private val artifactMetadataService: ArtifactMetadataService = mockk {
    coEvery {
      getArtifactMetadata("1", any())
    } returns artifactMetadata
  }

  private val artifactSupplier = object : BaseArtifactSupplier<DummyArtifact, DummySortingStrategy>(artifactMetadataService) {
    override val eventPublisher: EventPublisher = mockk()

    override val supportedArtifact: SupportedArtifact<DummyArtifact>
      get() = SupportedArtifact("dummy", DummyArtifact::class.java)

    override fun getLatestArtifact(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact): PublishedArtifact? {
      TODO("Not yet implemented")
    }

    override fun getLatestArtifacts(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, limit: Int): List<PublishedArtifact> {
      TODO("Not yet implemented")
    }

    override fun getArtifactByVersion(artifact: DeliveryArtifact, version: String): PublishedArtifact? {
      TODO("Not yet implemented")
    }

    override fun shouldProcessArtifact(artifact: PublishedArtifact): Boolean {
      return artifact.type == "dummy"
    }
  }

  private val artifact = DummyArtifact()
    .toArtifactVersion("1.0.0")
    .copy(buildMetadata = artifactMetadata.buildMetadata, gitMetadata = artifactMetadata.gitMetadata)

  @Test
  fun `returns artifact metadata from CI provider based on a regular commit`() {
    val result = runBlocking {
      artifactSupplier.getArtifactMetadata(artifact)
    }

    expectThat(result)
      .isEqualTo(artifactMetadata)
  }

  @Test
  fun `returns artifact metadata from CI provider based on a merge commit`() {
    val prCommitId = "12a45a"

    val result = runBlocking {
      artifactSupplier.getArtifactMetadata(
        artifact.copy(
          metadata = artifact.metadata + mapOf(
            "prCommitId" to prCommitId
          )
        )
      )
    }

    expectThat(result)
      .isEqualTo(artifactMetadata)

    coVerify(exactly = 1) {
      artifactSupplier.artifactMetadataService.getArtifactMetadata("1", prCommitId)
    }

    coVerify(exactly = 0) {
      artifactSupplier.artifactMetadataService.getArtifactMetadata("1", commitHash)
    }
  }
}
