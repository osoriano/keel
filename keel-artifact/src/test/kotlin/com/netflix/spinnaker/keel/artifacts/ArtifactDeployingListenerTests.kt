package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.events.ArtifactVersionDeploying
import com.netflix.spinnaker.keel.api.events.ArtifactVersionMarkedDeploying
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.test.DummyResourceSpec
import com.netflix.spinnaker.keel.test.deliveryConfig
import com.netflix.spinnaker.keel.test.resource
import com.netflix.spinnaker.time.MutableClock
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class ArtifactDeployingListenerTests {
  private val resource = resource()
  private val resourceSpy: Resource<DummyResourceSpec> = spyk(resource)
  private val config = deliveryConfig(resource = resourceSpy)
  private val artifact = config.artifacts.first()
  private val repository = mockk<KeelRepository>(relaxUnitFun = true)
  private val clock = MutableClock()
  private val publisher = mockk<EventPublisher>()

  private val event = ArtifactVersionDeploying(
    resourceId = resourceSpy.id,
    artifactVersion = "1.1.1"
  )

  private val subject = ArtifactDeployingListener(repository, publisher, clock)

  @BeforeEach
  fun setup() {
    every { repository.getResource(resource.id) } returns resourceSpy
    every { repository.deliveryConfigFor(resource.id) } returns config
    every { repository.environmentFor(resource.id) } returns config.environments.first()
    every { repository.isApprovedFor(any(), any(), event.artifactVersion, any()) } returns true
    every { resourceSpy.findAssociatedArtifact(config) } returns artifact
    every { publisher.publishEvent(any()) } just runs
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
  fun `publishes a telemetry event when version is approved`() {
    subject.onArtifactVersionDeploying(event)
    verify {
      publisher.publishEvent(ofType<ArtifactVersionMarkedDeploying>())
    }
  }

  @Test
  fun `publishes a telemetry event when version is pinned`() {
    subject.onArtifactVersionDeploying(event)
    verify {
      publisher.publishEvent(ofType<ArtifactVersionMarkedDeploying>())
    }
  }
}
