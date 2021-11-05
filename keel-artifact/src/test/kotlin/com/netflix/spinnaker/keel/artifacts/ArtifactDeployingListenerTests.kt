package com.netflix.spinnaker.keel.artifacts

import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Tag
import com.netflix.spectator.api.Timer
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.events.ArtifactVersionDeploying
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.telemetry.ARTIFACT_DELAY
import com.netflix.spinnaker.keel.test.DummyResourceSpec
import com.netflix.spinnaker.keel.test.deliveryConfig
import com.netflix.spinnaker.keel.test.resource
import com.netflix.spinnaker.time.MutableClock
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.one
import java.time.Duration

internal class ArtifactDeployingListenerTests {
  private val resource = resource()
  private val resourceSpy: Resource<DummyResourceSpec> = spyk(resource)
  private val config = deliveryConfig(resource = resourceSpy)
  private val artifact = config.artifacts.first()
  private val repository = mockk<KeelRepository>(relaxUnitFun = true)
  private val spectator = mockk<Registry>()
  private val clock = MutableClock()
  private val timer = mockk<Timer>()

  private val event = ArtifactVersionDeploying(
    resourceId = resourceSpy.id,
    artifactVersion = "1.1.1"
  )

  private val subject = ArtifactDeployingListener(repository, spectator, clock)

  @BeforeEach
  fun setup() {
    every { repository.getResource(resource.id) } returns resourceSpy
    every { repository.deliveryConfigFor(resource.id) } returns config
    every { repository.environmentFor(resource.id) } returns config.environments.first()
    every { repository.isApprovedFor(any(), any(), event.artifactVersion, any()) } returns true
    every { repository.getApprovedAt(any(), any(), event.artifactVersion, any()) } returns clock.instant()
    every { resourceSpy.findAssociatedArtifact(config) } returns artifact
    every { spectator.timer(any(), any<Iterable<Tag>>()) } returns timer
    every { timer.record(any<Duration>()) } just runs
  }

  @Test
  fun `does nothing if no artifact is associated with the resource`() {
    every { resourceSpy.findAssociatedArtifact(config) } returns null
    subject.onArtifactVersionDeploying(event)
    verify(exactly = 0) { repository.markAsDeployingTo(config, any(), event.artifactVersion, any()) }
  }

  @Test
  fun `does nothing if artifact version is not approved for env`() {
    every { repository.isApprovedFor(any(), any(), event.artifactVersion, any()) } returns false
    subject.onArtifactVersionDeploying(event)
    verify(exactly = 0) { repository.markAsDeployingTo(config, any(), event.artifactVersion, any()) }
  }

  @Test
  fun `marks version as deploying when artifact version is approved for env`() {
    subject.onArtifactVersionDeploying(event)
    verify(exactly = 1) { repository.markAsDeployingTo(any(), any(), event.artifactVersion, any()) }
  }

  @Test
  fun `records deployment delay when version is approved`() {
    subject.onArtifactVersionDeploying(event)

    val tags = slot<Iterable<Tag>>()

    verify { spectator.timer(ARTIFACT_DELAY, capture(tags)) }
    verify { timer.record(any<Duration>()) }

    expectThat(tags.captured) {
      one {
        get { key() }.isEqualTo("delayType")
        get { value() }.isEqualTo("deployment")
      }
      one {
        get { key() }.isEqualTo("artifactType")
      }
      one {
        get { key() }.isEqualTo("artifactName")
      }
    }
  }
}
