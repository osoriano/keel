package com.netflix.spinnaker.keel.artifacts

import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.artifacts.ArtifactMetadata
import com.netflix.spinnaker.keel.api.artifacts.ArtifactStatus
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.Job
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.Repo
import com.netflix.spinnaker.keel.api.events.ArtifactVersionStored
import com.netflix.spinnaker.keel.api.plugins.SupportedArtifact
import com.netflix.spinnaker.keel.config.WorkProcessingConfig
import com.netflix.spinnaker.keel.igor.BuildService
import com.netflix.spinnaker.keel.igor.model.ArtifactContents
import com.netflix.spinnaker.keel.igor.model.BuildDetail
import com.netflix.spinnaker.keel.igor.model.TriggerEvent
import com.netflix.spinnaker.keel.lifecycle.LifecycleEvent
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventStatus
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.test.debianArtifact
import com.netflix.spinnaker.keel.test.dockerArtifact
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.Called
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import org.springframework.context.ApplicationEventPublisher
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import strikt.assertions.isTrue
import strikt.assertions.one
import java.time.Clock
import io.mockk.coEvery as every
import io.mockk.coVerify as verify

internal class ArtifactQueueProcessorTest : JUnit5Minutests {

  val branchName = "super-branch"
  val branchNameFromRocket = "super-branch-from-rocket"
  val publishedDeb = PublishedArtifact(
    type = "DEB",
    customKind = false,
    name = "fnord",
    version = "0.156.0-h58.f67fe09",
    reference = "debian-local:pool/f/fnord/fnord_0.156.0-h58.f67fe09_all.deb",
    metadata = mapOf("releaseStatus" to ArtifactStatus.FINAL, "buildNumber" to "58", "commitId" to "f67fe09", "branch" to branchName),
    provenance = "https://my.jenkins.master/jobs/fnord-release/58",
    buildMetadata = BuildMetadata(
      id = 58,
      number = "58",
      status = "BUILDING",
      uid = "i-am-a-uid-obviously"
    )
  ).normalized()

  val newerPublishedDeb = PublishedArtifact(
    type = "DEB",
    customKind = false,
    name = "fnord",
    version = "0.161.0-h61.116f116",
    reference = "debian-local:pool/f/fnord/fnord_0.161.0-h61.116f116_all.deb",
    metadata = mapOf("releaseStatus" to ArtifactStatus.FINAL, "buildNumber" to "61", "commitId" to "116f116", "branch" to branchName),
    provenance = "https://my.jenkins.master/jobs/fnord-release/60",
    buildMetadata = BuildMetadata(
      id = 58,
      number = "58",
      status = "BUILDING",
      uid = "just-a-uid-obviously"
    )
  ).normalized()

  val imageProperties = """
    imageID=sha256:409a2ea5e8120891ff1ce32b3fb9684237a7b292a7b01871512713f4f836f14a
    imageName=lpollo/lpollo-local-test:master-h232.ff1f4d4
  """.trimIndent()

  val debianArtifact = debianArtifact()
  val dockerArtifact = dockerArtifact()
  val deliveryConfig = DeliveryConfig(
    name = "fnord-config",
    application = "fnord",
    serviceAccount = "keel",
    artifacts = setOf(debianArtifact, dockerArtifact)
  )

  val artifactMetadata = ArtifactMetadata(
    gitMetadata = GitMetadata(commit = "f00baah", author = "joesmith", branch = branchNameFromRocket, repo = Repo(name = "awesome-name")),
    buildMetadata = BuildMetadata(id = 1, status = "SUCCEEDED")
  )

  abstract class EventQueueProcessorFixture {
    val repository: KeelRepository = mockk(relaxUnitFun = true)
    val buildService: BuildService = mockk()
    val objectMapper = configuredObjectMapper()
    val publisher: ApplicationEventPublisher = mockk(relaxUnitFun = true)
    val dockerArtifactSupplier: DockerArtifactSupplier = mockk(relaxUnitFun = true) {
      every { shouldProcessArtifact(any()) } returns true
      every { supportedArtifact } returns SupportedArtifact(DOCKER, DockerArtifact::class.java)
    }
    val debianArtifactSupplier: DebianArtifactSupplier = mockk(relaxUnitFun = true) {
      every { shouldProcessArtifact(any()) } returns true
      every { supportedArtifact } returns SupportedArtifact(DEBIAN, DebianArtifact::class.java)
    }
    val artifactSuppliers = listOf(dockerArtifactSupplier, debianArtifactSupplier)
    val spectator = NoopRegistry()
    val clock: Clock = MutableClock()

    val subject = spyk(
      ArtifactQueueProcessor(
        config = WorkProcessingConfig(),
        repository = repository,
        buildService = buildService,
        artifactSuppliers = artifactSuppliers,
        spectator = spectator,
        clock = clock,
        publisher = publisher,
        objectMapper = objectMapper
      )
    )

    val PublishedArtifact.buildDetail: BuildDetail?
      get() = metadata["buildDetail"]?.let { objectMapper.convertValue<BuildDetail>(it) }

    private val PublishedArtifact.buildTriggerEvent: TriggerEvent?
      get() = metadata["triggerEvent"]?.let { objectMapper.convertValue<TriggerEvent>(it) }
  }

  data class ArtifactPublishedFixture(
    val version: PublishedArtifact,
    val artifact: DeliveryArtifact
  ) : EventQueueProcessorFixture()

  fun artifactEventTests() = rootContext<ArtifactPublishedFixture> {
    fixture {
      ArtifactPublishedFixture(
        version = publishedDeb,
        artifact = debianArtifact
      )
    }

    before {
      every { repository.getDeliveryConfig(any()) } returns deliveryConfig
      every { debianArtifactSupplier.supportedArtifact } returns SupportedArtifact(DEBIAN, DebianArtifact::class.java)
      every { dockerArtifactSupplier.supportedArtifact } returns SupportedArtifact(DOCKER, DockerArtifact::class.java)
    }

    context("the artifact is not something we're tracking") {
      before {
        every { repository.isRegistered(any(), any()) } returns false
        subject.handlePublishedArtifact(version)
      }

      test("the event is ignored") {
        verify(exactly = 0) { repository.storeArtifactVersion(any()) }
      }

      test("no telemetry is recorded") {
        verify { publisher wasNot Called }
      }
    }

    context("the artifact is registered with versions") {
      before {
        every { repository.isRegistered(artifact.name, artifact.type) } returns true
        every {
          debianArtifactSupplier.getLatestVersion(deliveryConfig, artifact)
        } returns publishedDeb
        every {
          debianArtifactSupplier.getArtifactMetadata(publishedDeb)
        } returns artifactMetadata
        every {
          debianArtifactSupplier.shouldProcessArtifact(any())
        } returns true
      }

      context("the version was already known") {
        before {
          every { repository.storeArtifactVersion(any()) } returns false
          every { repository.getAllArtifacts(DEBIAN, any()) } returns listOf(debianArtifact)

          subject.handlePublishedArtifact(version)
        }

        test("only lifecycle event recorded") {
          verify(exactly = 1) { publisher.publishEvent(ofType<LifecycleEvent>()) }
          verify(exactly = 0) { publisher.publishEvent(ofType<ArtifactVersionStored>()) }
        }
      }

      context("the version is new") {
        before {
          every {
            debianArtifactSupplier.getArtifactMetadata(any())
          } returns artifactMetadata

          every { repository.storeArtifactVersion(any()) } returns true
          every { repository.getAllArtifacts(DEBIAN, any()) } returns listOf(debianArtifact)
          subject.handlePublishedArtifact(newerPublishedDeb)
        }

        val artifactVersions = mutableListOf<PublishedArtifact>()
        test("a new artifact version is stored; store before adding more metadata") {
          verify(exactly = 2) { repository.storeArtifactVersion(capture(artifactVersions)) }

          val normalizedArtifact = newerPublishedDeb.normalized()
          with(artifactVersions.first()) {
            expectThat(name).isEqualTo(artifact.name)
            expectThat(type).isEqualTo(normalizedArtifact.type)
            expectThat(version).isEqualTo(normalizedArtifact.version)
            expectThat(status).isEqualTo(ArtifactStatus.FINAL)
            expectThat(branch).isEqualTo(branchName)
            expectThat(gitMetadata?.repo).isNull()
          }
        }

        test("artifact metadata is added from rocket") {
          verify(exactly = 1) {
            debianArtifactSupplier.getArtifactMetadata(any())
          }

          with(artifactVersions[1]) {
            expectThat(gitMetadata?.branch).isEqualTo(branchNameFromRocket)
            expectThat(gitMetadata?.repo?.name).isEqualTo("awesome-name")
          }
        }

        test("a telemetry event is published") {
          verify {
            publisher.publishEvent(ofType<ArtifactVersionStored>())
          }
        }
      }
    }

    context("don't add metadata if the artifact metadata is already complete") {
      before {
        every { repository.isRegistered(artifact.name, artifact.type) } returns true
        every {
          debianArtifactSupplier.getLatestVersion(deliveryConfig, artifact)
        } returns publishedDeb
        every {
          debianArtifactSupplier.getArtifactMetadata(publishedDeb)
        } returns artifactMetadata
        every {
          debianArtifactSupplier.shouldProcessArtifact(any())
        } returns true
        every { repository.storeArtifactVersion(any()) } returns true
        every { repository.getAllArtifacts(DEBIAN, any()) } returns listOf(debianArtifact)
      }

      test("don't call rocket if all the metadata is there") {
        val completeArtifact = newerPublishedDeb.copy(
          buildMetadata = BuildMetadata(
            status = "RUNNING",
            id = 13,
            job = Job(link = "www.job", name = "awesome-job"),
            number = "13"
          ),
          gitMetadata = GitMetadata(
            repo = Repo(name = "blabla"),
            commit = "blabla",
            branch = branchName,
            project = "this",
            commitInfo = Commit(sha = "blabla")
          )
        )
        subject.handlePublishedArtifact(completeArtifact)

        verify(exactly = 0) {
          debianArtifactSupplier.getArtifactMetadata(completeArtifact)
        }

        verify(exactly = 1) { repository.storeArtifactVersion(completeArtifact) }
      }
    }

    context("rocket is down") {
      before {
        every { debianArtifactSupplier.getArtifactMetadata(any()) } throws Exception()
        every { repository.storeArtifactVersion(any()) } returns true
        every { repository.getAllArtifacts(DEBIAN, any()) } returns listOf(debianArtifact)
        every { repository.isRegistered(artifact.name, artifact.type) } returns true
        every { debianArtifactSupplier.shouldProcessArtifact(any()) } returns true
      }
      test("use basic artifact info") {
        subject.handlePublishedArtifact(newerPublishedDeb)
        val completeArtifact = slot<PublishedArtifact>()

        verify(exactly = 1) {
          repository.storeArtifactVersion(capture(completeArtifact))
          debianArtifactSupplier.getArtifactMetadata(any())
        }
        with(completeArtifact.captured) {
          expectThat(branch).isEqualTo(branchName)
        }
      }
    }

    context("a Rocket build event for Docker is disguised as an artifact") {
      before {
        every {
          repository.isRegistered(any(), any())
        } returns true

        every {
          subject.enrichAndStore(any(), any())
        } returns true

        every {
          buildService.getArtifactContents(any(), any(), any(), any())
        } returns ArtifactContents(imageProperties.toByteArray())
      }

      test("completes missing Docker image data in the artifact before handling") {
        subject.handlePublishedArtifact(disguisedPublishedDocker)

        verify {
          with(disguisedPublishedDocker) {
            buildService.getArtifactContents(
              buildController!!, buildJob!!, buildDetail!!.buildNumber,
              "lpollo-local-test-server/build/image-server.properties"
            )
          }
        }

        val completeArtifact = slot<PublishedArtifact>()
        verify {
          subject.enrichAndStore(capture(completeArtifact), any())
        }

        expectThat(completeArtifact.captured) {
          get { type }.isEqualTo(DOCKER)
          get { name }.isEqualTo("lpollo/lpollo-local-test")
          get { version }.isEqualTo("master-h232.ff1f4d4")
          get { createdAt }.isNotNull()
          get { metadata["commitId"] }.isEqualTo(disguisedPublishedDocker.buildDetail!!.commitId)
          get { metadata["buildNumber"] }.isEqualTo(disguisedPublishedDocker.buildDetail!!.buildNumber.toString())
          get { branch }.isEqualTo(branchName)
        }
      }
    }
  }

  data class LifecycleEventsFixture(
    val debArtifact: DeliveryArtifact,
    val dockerArtifact: DockerArtifact
  ) : EventQueueProcessorFixture()

  fun lifecyclePublishingEventTests() = rootContext<LifecycleEventsFixture> {
    fixture {
      LifecycleEventsFixture(
        debArtifact = debianArtifact,
        dockerArtifact = dockerArtifact
      )
    }

    before {
      every { repository.getDeliveryConfig(any()) } returns deliveryConfig
      every { debianArtifactSupplier.supportedArtifact } returns SupportedArtifact(DEBIAN, DebianArtifact::class.java)
      every { dockerArtifactSupplier.supportedArtifact } returns SupportedArtifact(DOCKER, DockerArtifact::class.java)
      every { debianArtifactSupplier.getArtifactMetadata(any()) } returns artifactMetadata
      every { dockerArtifactSupplier.getArtifactMetadata(any()) } returns artifactMetadata
      every { repository.storeArtifactVersion(any()) } returns true
    }

    context("events") {
      context("multiple artifacts") {
        before {
          every { repository.getAllArtifacts(DEBIAN, any()) } returns
            listOf(
              debianArtifact,
              debianArtifact.copy(reference = "blah-blay", deliveryConfigName = "another-config")
            )
          subject.findArtifactsAndPublishEvent(publishedDeb)
        }

        test("publishes build lifecycle event for each artifact") {
          verify(exactly = 2) { publisher.publishEvent(ofType<LifecycleEvent>()) }
        }
      }

      context("single artifact") {
        before {
          every { repository.getAllArtifacts(DEBIAN, any()) } returns
            listOf(debianArtifact)
          subject.findArtifactsAndPublishEvent(publishedDeb)
        }

        test("publishes build lifecycle event with monitor = true") {
          val events = mutableListOf<Any>()
          verify(exactly = 1) { publisher.publishEvent(capture(events)) }
          expectThat(events).one {
            isA<LifecycleEvent>().and {
              get { status }.isEqualTo(LifecycleEventStatus.RUNNING)
              get { startMonitoring }.isTrue()
            }
          }
        }

        test("publishes artifact version detected event with the right information") {
          val events = mutableListOf<Any>()
          verify(exactly = 1) { publisher.publishEvent(capture(events)) }
          expectThat(events).one {
            isA<LifecycleEvent>().and {
              get { deliveryConfig }.isEqualTo(deliveryConfig)
              get { artifactReference }.isEqualTo(debianArtifact.reference)
              get { artifactVersion }.isEqualTo(publishedDeb.version)
            }
          }
        }
      }
    }
  }
}
