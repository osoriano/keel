package com.netflix.spinnaker.keel.artifacts

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.keel.api.artifacts.ArtifactStatus
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.config.WorkProcessingConfig
import com.netflix.spinnaker.keel.persistence.WorkQueueRepository
import com.netflix.spinnaker.keel.scm.CodeEvent
import com.netflix.spinnaker.keel.scm.CommitCreatedEvent
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.context.ApplicationEventPublisher
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment
import java.time.Clock

class SqlWorkQueueConsumerTest {

  private val artifactQueueProcessor: ArtifactQueueProcessor = mockk(relaxUnitFun = true)
  private val drainer: SqlWorkQueueDrainer = mockk(relaxUnitFun = true)
  private val workQueueRepository: WorkQueueRepository = mockk()
  private val clock = Clock.systemUTC()
  private val spectator = NoopRegistry()
  private val springEnv = StandardEnvironment().apply {
    propertySources.addFirst(
      MapPropertySource(
        "test",
        mapOf(
          "keel.work-processing.draining-enabled" to false
        )
      )
    )
  }
  private val config: WorkProcessingConfig = WorkProcessingConfig()
  private val publisher: ApplicationEventPublisher = mockk(relaxUnitFun = true)
  private val subject = SqlWorkQueueConsumer(artifactQueueProcessor, drainer, workQueueRepository, clock, spectator, springEnv, config, publisher)

  @Test
  fun `should no-op when disabled`() {
    subject.enabled.set(false)
    subject.consumeArtifactQueue()
    subject.consumeCodeEventQueue()

    verify(exactly = 0) { workQueueRepository.removeArtifactsFromQueue(any()) }
    verify(exactly = 0) { workQueueRepository.removeCodeEventsFromQueue(any()) }
    verify(exactly = 0) { publisher.publishEvent(any()) }
    coVerify(exactly = 0) { artifactQueueProcessor.processArtifact(any()) }
  }

  @Test
  fun `should do nothing when no artifacts are available`() {
    every { workQueueRepository.removeArtifactsFromQueue(any()) } returns emptyList()

    subject.enabled.set(true)
    subject.consumeArtifactQueue()

    coVerify(exactly = 0) { artifactQueueProcessor.processArtifact(any()) }
  }

  @Test
  fun `should process artifact when artifacts are consumed`() {
    every { workQueueRepository.removeArtifactsFromQueue(any()) } returns listOf(
      PublishedArtifact(
        type = "DEB",
        customKind = false,
        name = "fnord",
        version = "0.156.0-h58.f67fe09",
        reference = "debian-local:pool/f/fnord/fnord_0.156.0-h58.f67fe09_all.deb",
        metadata = mapOf("releaseStatus" to ArtifactStatus.FINAL, "buildNumber" to "58", "commitId" to "f67fe09", "branch" to "main"),
        provenance = "https://my.jenkins.master/jobs/fnord-release/58",
        buildMetadata = BuildMetadata(
          id = 58,
          number = "58",
          status = "BUILDING",
          uid = "i-am-a-uid-obviously"
        )
      ).normalized()
    )

    subject.enabled.set(true)
    subject.consumeArtifactQueue()

    coVerify { artifactQueueProcessor.processArtifact(any()) }
  }

  @Test
  fun `should no-op when no code events are available`() {
    every { workQueueRepository.removeCodeEventsFromQueue(any()) } returns emptyList()

    subject.enabled.set(true)
    subject.consumeCodeEventQueue()

    verify(exactly = 0) { publisher.publishEvent(any()) }
  }

  @Test
  fun `should consume code events`() {
    every { workQueueRepository.removeCodeEventsFromQueue(any()) } returns listOf(
      CommitCreatedEvent("git/spkr/keel-nflx", "main", "spkr", "keel-nflx", commitHash = "abcd1234")
    )

    subject.enabled.set(true)
    subject.consumeCodeEventQueue()

    verify { publisher.publishEvent(any<CodeEvent>()) }
  }

  @Test
  fun `should invoke drainer when present`() {
    every { workQueueRepository.removeArtifactsFromQueue(any()) } returns listOf(
      PublishedArtifact(
        type = "DEB",
        customKind = false,
        name = "fnord",
        version = "0.156.0-h58.f67fe09",
        reference = "debian-local:pool/f/fnord/fnord_0.156.0-h58.f67fe09_all.deb",
        metadata = mapOf("releaseStatus" to ArtifactStatus.FINAL, "buildNumber" to "58", "commitId" to "f67fe09", "branch" to "main"),
        provenance = "https://my.jenkins.master/jobs/fnord-release/58",
        buildMetadata = BuildMetadata(
          id = 58,
          number = "58",
          status = "BUILDING",
          uid = "i-am-a-uid-obviously"
        )
      ).normalized()
    )
    springEnv.propertySources.remove("test")
    springEnv.propertySources.addFirst(MapPropertySource("test", mapOf("keel.work-processing.draining-enabled" to true)))

    val subject = SqlWorkQueueConsumer(artifactQueueProcessor, drainer, workQueueRepository, clock, spectator, springEnv, config, publisher)

    subject.enabled.set(true)
    subject.consumeArtifactQueue()

    coVerify(exactly = 1) { drainer.drainToSqs(any()) }
    coVerify(exactly = 0) { artifactQueueProcessor.processArtifact(any()) }
  }
}
