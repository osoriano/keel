package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.constraints.ConstraintState
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.api.events.ArtifactVersionDeployed
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.core.api.PromotionStatus.CURRENT
import com.netflix.spinnaker.keel.core.api.PromotionStatus.PENDING
import com.netflix.spinnaker.keel.events.ArtifactDeployedNotification
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.test.DummyResourceSpec
import com.netflix.spinnaker.keel.test.deliveryConfig
import com.netflix.spinnaker.keel.test.resource
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.Called
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

internal class ArtifactDeployedListenerTests {

  val resource = resource()
  val resourceSpy: Resource<DummyResourceSpec> = spyk(resource)
  val config = deliveryConfig(resource = resourceSpy)
  val artifact = config.artifacts.first()
  val repository = mockk<KeelRepository>(relaxUnitFun = true)
  val publisher: EventPublisher = mockk(relaxUnitFun = true)

  val event = ArtifactVersionDeployed(
    resourceId = resourceSpy.id,
    artifactVersion = "1.1.1"
  )

  val publishedArtifact = PublishedArtifact(
    name = artifact.name,
    type = artifact.type,
    version = event.artifactVersion,
    reference = artifact.reference,
  )

  val event2 = event.copy(artifactVersion = "1.1.2")

  val subject = ArtifactDeployedListener(repository, publisher)

  @BeforeEach
  fun setup() {
    every { repository.getResource(resource.id) } returns resourceSpy
    every { repository.deliveryConfigFor(resource.id) } returns config
    every { repository.environmentFor(resource.id) } returns config.environments.first()
  }

  @AfterEach
  fun cleanup() {
    clearAllMocks()
  }

  @Test
  fun `no artifact associated with the resource - nothing is marked as deployed`() {
    subject.onArtifactVersionDeployed(event)
    verify(exactly = 0) { repository.isApprovedFor(config, any(), event.artifactVersion, any()) }
    verify(exactly = 0) { repository.markAsSuccessfullyDeployedTo(config, any(), event.artifactVersion, any()) }
  }

  @Nested
  @DisplayName("artifact is approved for env")
  inner class ArtifactIsApprovedForEnv {
    @BeforeEach
    fun setup() {
      every { repository.getLatestApprovedInEnvArtifactVersion(any(), any(), any(), any()) } returns publishedArtifact
      every { resourceSpy.findAssociatedArtifact(any()) } returns artifact
      every { repository.isApprovedFor(any(), any(), event.artifactVersion, any()) } returns true
    }


    @Test
    fun `artifact has been marked currently deployed - artifact is not marked as deployed again`() {
      every { repository.markAsSuccessfullyDeployedTo(config, any(), event.artifactVersion, any()) } returns false
      subject.onArtifactVersionDeployed(event)
      verify { publisher wasNot Called }
    }

    @Test
    fun `artifact has not been marked currently deployed`() {
      every { repository.markAsSuccessfullyDeployedTo(config, any(), event.artifactVersion, any()) } returns true
      subject.onArtifactVersionDeployed(event)
      verify(exactly = 1) { repository.markAsSuccessfullyDeployedTo(any(), any(), event.artifactVersion, any()) }
      verify { publisher.publishEvent(ofType<ArtifactDeployedNotification>()) }
    }
  }

  @Nested
  @DisplayName("artifact is not approved for env")
  inner class ArtifactIsNotApprovedForEnv {
    @BeforeEach
    fun setup() {
      every { repository.getLatestApprovedInEnvArtifactVersion(any(), any(), any(), any()) } returns publishedArtifact
      every { resourceSpy.findAssociatedArtifact(any()) } returns artifact
      every { repository.isApprovedFor(any(), any(), event.artifactVersion, any()) } returns false
    }

    @Test
    fun `existing app`() {
      subject.onArtifactVersionDeployed(event)
      verify(exactly = 0) { repository.markAsSuccessfullyDeployedTo(config, any(), event.artifactVersion, any()) }
      verify { publisher wasNot Called }
    }
  }

  @Nested
  @DisplayName("approve versions when onboarding new apps")
  inner class OnboardingNewApps {
    @BeforeEach
    fun setup() {
      every { repository.getLatestApprovedInEnvArtifactVersion(any(), any(), any(), any()) } returns publishedArtifact
      every { resourceSpy.findAssociatedArtifact(any()) } returns artifact
      every { repository.constraintStateFor(any(), any(), any(), any()) } returns listOf(
        ConstraintState(
          deliveryConfigName = config.name,
          environmentName = "test",
          artifactVersion = event.artifactVersion,
          artifactReference = artifact.reference,
          type = "manual-judgement",
          status = ConstraintStatus.PENDING,
        )
      )
      every { repository.storeConstraintState(any()) } just runs
      every { repository.markAsSuccessfullyDeployedTo(any(), any(), any(), any()) } returns true
    }

    @Test
    fun `first onboarding - no approved versions, marking the version as deployed`() {
      every { repository.getLatestApprovedInEnvArtifactVersion(any(), any(), any(), any()) } returns null
      every { repository.isApprovedFor(any(), any(), event.artifactVersion, any()) } returns false

      subject.onArtifactVersionDeployed(event)
      verify(exactly = 1) { repository.markAsSuccessfullyDeployedTo(any(), any(), event.artifactVersion, any()) }
      verify(exactly = 1) { repository.storeConstraintState(any()) }
      verify(exactly = 1) { publisher.publishEvent(ofType<ArtifactDeployedNotification>()) }
    }

    @Test
    fun `keep updating new version while app is migrating`() {
      every { repository.getLatestApprovedInEnvArtifactVersion(any(), any(), any(), any()) } returns publishedArtifact
      every { repository.deliveryConfigFor(resource.id) } returns config.copy(metadata = mapOf(DeliveryConfig.MIGRATING_KEY to true))
      every { repository.isApprovedFor(any(), any(), event.artifactVersion, any()) } returns true
      every { repository.isApprovedFor(any(), any(), event2.artifactVersion, any()) } returns false

      subject.onArtifactVersionDeployed(event2)
      verify(exactly = 1) { repository.markAsSuccessfullyDeployedTo(any(), any(), event2.artifactVersion, any()) }
      verify(exactly = 1) { repository.storeConstraintState(any()) }
      verify(exactly = 1) { publisher.publishEvent(ofType<ArtifactDeployedNotification>()) }
    }
  }

}
