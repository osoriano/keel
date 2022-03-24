package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.events.ArtifactPublishedEvent
import com.netflix.spinnaker.keel.artifacts.WorkQueueProcessor
import com.netflix.spinnaker.keel.igor.artifact.ArtifactMetadataService
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.context.ApplicationEventPublisher
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isSuccess

internal class ArtifactControllerTests {

  private val workQueueProcessor: WorkQueueProcessor = mockk {
    every { queueCodeEventForProcessing(any()) } just Runs
    every { queueArtifactForProcessing(any()) } just Runs
  }

  private val artifactMetadataService: ArtifactMetadataService = mockk()

  private val featureToggles: FeatureToggles = mockk {
    every { isEnabled(any(), any()) } returns true
  }

  private val subject = ArtifactController(artifactMetadataService, workQueueProcessor, featureToggles)

  private val disguisedCodeEvent = EchoArtifactEvent(
    eventName = "test",
    payload = ArtifactPublishedEvent(
      artifacts = listOf(
        PublishedArtifact(
          name = "master:953910b24a776eceab03d4dcae8ac050b2e0b668",
          type = "pr_opened",
          reference = "https://stash/projects/ORG/repos/myrepo/commits/953910b24a776eceab03d4dcae8ac050b2e0b668",
          version = "953910b24a776eceab03d4dcae8ac050b2e0b668",
          provenance = "https://stash/projects/ORG/repos/myrepo/commits/953910b24a776eceab03d4dcae8ac050b2e0b668",
          metadata = mapOf(
            "rocketEventType" to "CODE",
            "repoKey" to "stash/org/myrepo",
            "prId" to "11494",
            "sha" to  "953910b24a776eceab03d4dcae8ac050b2e0b668",
            "branch" to "master",
            "prBranch" to "feature/branch",
            "targetBranch" to "master",
            "originalPayload" to mapOf(
              "causedBy" to mapOf(
                "email" to "keel@keel"
              ),
              "target" to mapOf(
                "projectKey" to "org",
                "repoName" to "myrepo"
              )
            )
          )
        )
      )
    )
  )

  private val disguisedDockerBuildEvent = EchoArtifactEvent(
    eventName = "test",
    payload = ArtifactPublishedEvent(
      artifacts = listOf(
        PublishedArtifact(
          name = "See image.properties",
          type = "docker",
          reference = "image.properties",
          version = "See image.properties",
          metadata = mapOf(
            "rocketEventType" to "BUILD",
            "buildDetail" to mapOf(
              "result" to "SUCCESSFUL"
            )
          )
        )
      )
    )
  )

  private val dockerBuildFailedEvent = EchoArtifactEvent(
    eventName = "test",
    payload = ArtifactPublishedEvent(
      artifacts = listOf(
        PublishedArtifact(
          name = "See image.properties",
          type = "docker",
          reference = "image.properties",
          version = "See image.properties",
          metadata = mapOf(
            "rocketEventType" to "BUILD",
            "buildDetail" to mapOf(
              "result" to "FAILED"
            )
          )
        )
      )
    )
  )

  @Test
  fun `a code event disguised as an artifact event is translated and queued as code event`() {
    expectCatching {
      subject.submitArtifact(disguisedCodeEvent)
    }.isSuccess()


    verify(exactly = 1) {
      workQueueProcessor.queueCodeEventForProcessing(any())
    }

    verify(exactly = 0) {
      workQueueProcessor.queueArtifactForProcessing(any())
    }
  }

  @Test
  fun `a successful Docker build event disguised as an artifact event is queued as an artifact event`() {
    val queuedArtifact = slot<PublishedArtifact>()

    expectCatching {
      subject.submitArtifact(disguisedDockerBuildEvent)
    }.isSuccess()

    verify(exactly = 1) {
      workQueueProcessor.queueArtifactForProcessing(capture(queuedArtifact))
    }

    expectThat(queuedArtifact.captured) {
      get { type }.isEqualTo(DOCKER)
    }
  }

  @Test
  fun `a failed Docker build event disguised as an artifact event is not queued as an artifact event`() {
    expectCatching {
      subject.submitArtifact(dockerBuildFailedEvent)
    }.isSuccess()

    verify(exactly = 0) {
      workQueueProcessor.queueArtifactForProcessing(any())
    }
  }
}
