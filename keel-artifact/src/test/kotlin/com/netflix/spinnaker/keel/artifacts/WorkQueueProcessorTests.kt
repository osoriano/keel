package com.netflix.spinnaker.keel.artifacts

import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.artifacts.ArtifactMetadata
import com.netflix.spinnaker.keel.api.artifacts.ArtifactStatus
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.events.ArtifactVersionDetected
import com.netflix.spinnaker.keel.api.plugins.SupportedArtifact
import com.netflix.spinnaker.keel.config.WorkProcessingConfig
import com.netflix.spinnaker.keel.igor.BuildService
import com.netflix.spinnaker.keel.igor.model.ArtifactContents
import com.netflix.spinnaker.keel.igor.model.BuildDetail
import com.netflix.spinnaker.keel.lifecycle.LifecycleEvent
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventStatus
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.WorkQueueRepository
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.telemetry.ArtifactVersionUpdated
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.Called
import io.mockk.coEvery as every
import io.mockk.coVerify as verify
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import org.springframework.context.ApplicationEventPublisher
import org.springframework.core.env.Environment
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isTrue
import strikt.assertions.one
import java.time.Clock

internal class WorkQueueProcessorTests : JUnit5Minutests {

  val publishedDeb = PublishedArtifact(
    type = "DEB",
    customKind = false,
    name = "fnord",
    version = "0.156.0-h58.f67fe09",
    reference = "debian-local:pool/f/fnord/fnord_0.156.0-h58.f67fe09_all.deb",
    metadata = mapOf("releaseStatus" to ArtifactStatus.FINAL, "buildNumber" to "58", "commitId" to "f67fe09"),
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
    metadata = mapOf("releaseStatus" to ArtifactStatus.FINAL, "buildNumber" to "61", "commitId" to "116f116"),
    provenance = "https://my.jenkins.master/jobs/fnord-release/60",
    buildMetadata = BuildMetadata(
      id = 58,
      number = "58",
      status = "BUILDING",
      uid = "just-a-uid-obviously"
    )
  ).normalized()

  private val buildUrl = "https://krypton.builds.test.netflix.net/job/USERS-lpollo-lpollo-local-test-build/230/"
  val disguisedPublishedDocker = PublishedArtifact(
    name = "See image.properties",
    type = "docker",
    reference = "image.properties",
    version = "See image.properties",
    metadata = mapOf(
      "controllerName" to "krypton",
      "jobName" to "users-lpollo-lpollo-local-test-build",
      "buildDetail" to  mapOf(
        "buildEngine" to "jenkins-krypton",
        "buildId" to "230",
        "buildDisplayName" to "USERS-lpollo-lpollo-local-test-build #230 - Branch: master",
        "buildDescription" to "msrc@netflix.com merged <a href=\"https://stash.corp.netflix.com/projects/~LPOLLO/repos/lpollo-local…",
        "result" to "SUCCESSFUL",
        "buildUrl" to buildUrl,
        "artifacts" to listOf(
          "$buildUrl/artifact/lpollo-local-test-client/build/reports/project/dependencies.txt",
          "$buildUrl/artifact/lpollo-local-test-client/build/reports/project/properties.txt",
          "$buildUrl/artifact/lpollo-local-test-proto-definition/build/reports/project/dependencies.txt",
          "$buildUrl/artifact/lpollo-local-test-proto-definition/build/reports/project/properties.txt",
          "$buildUrl/artifact/lpollo-local-test-server/build/distributions/lpollo-local-test-server_0.0.1%7Esnapshot-h230.88b3f71_all.deb",
          "$buildUrl/artifact/lpollo-local-test-server/build/image-server.properties",
          "$buildUrl/artifact/lpollo-local-test-server/build/reports/project/dependencies.txt",
          "$buildUrl/artifact/lpollo-local-test-server/build/reports/project/properties.txt",
          "$buildUrl/artifact/lpollo-local-test-server/build/reports/properties/properties-report-lpollolocaltest.txt",
          "$buildUrl/artifact/lpollo-local-test-server/build/reports/smokeTest/classes/com.netflix.lpollolocaltest.SmokeTest.html",
          "$buildUrl/artifact/lpollo-local-test-server/build/reports/smokeTest/css/base-style.css",
          "$buildUrl/artifact/lpollo-local-test-server/build/reports/smokeTest/css/style.css",
          "$buildUrl/artifact/lpollo-local-test-server/build/reports/smokeTest/index.html",
          "$buildUrl/artifact/lpollo-local-test-server/build/reports/smokeTest/js/report.js",
          "$buildUrl/artifact/lpollo-local-test-server/build/reports/smokeTest/packages/com.netflix.lpollolocaltest.html",
          "$buildUrl/artifact/lpollo-local-test-server/build/reports/startup/startup-report-lpollolocaltest-1.html"
        ),
        "logs" to listOf(
          "$buildUrl/consoleText"
        ),
        "reports" to emptyList<String>(),
        "buildNumber" to 230,
        "startedAt" to 1642470544511,
        "queuedAt" to 1642470544507,
        "completedAt" to 1642470752660,
        "commitId" to "88b3f7131aaea370b5649da695387e5e23c4053f",
        "agentId" to "nflx-agent-krypton-i-0ba8112ce613b7dff"
      )
    )
  )
  val imageProperties = """
    imageID=sha256:409a2ea5e8120891ff1ce32b3fb9684237a7b292a7b01871512713f4f836f14a
    imageName=lpollo/lpollo-local-test:master-h232.ff1f4d4    
  """.trimIndent()

  val debianArtifact = DebianArtifact(name = "fnord", deliveryConfigName = "fnord-config", vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2")))
  val dockerArtifact = DockerArtifact(name = "fnord/myimage", tagVersionStrategy = TagVersionStrategy.BRANCH_JOB_COMMIT_BY_JOB, deliveryConfigName = "fnord-config")
  val deliveryConfig = DeliveryConfig(name = "fnord-config", application = "fnord", serviceAccount = "keel", artifacts = setOf(debianArtifact, dockerArtifact))

  val artifactMetadata = ArtifactMetadata(
    gitMetadata = GitMetadata(commit = "f00baah", author = "joesmith", branch = "master"),
    buildMetadata = BuildMetadata(id = 1, status = "SUCCEEDED")
  )

  val artifactVersion = slot<PublishedArtifact>()

  abstract class EventQueueProcessorFixture {
    val repository: KeelRepository = mockk(relaxUnitFun = true)
    val workQueueRepository: WorkQueueRepository = mockk(relaxUnitFun = true)
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
    val springEnv: Environment = mockk(relaxed = true)

    val subject = spyk(
      WorkQueueProcessor(
        config = WorkProcessingConfig(),
        workQueueRepository = workQueueRepository,
        repository = repository,
        buildService = buildService,
        artifactSuppliers = artifactSuppliers,
        publisher = publisher,
        spectator = spectator,
        clock = clock,
        springEnv = springEnv,
        objectMapper = objectMapper
      )
    )

    val PublishedArtifact.buildDetail: BuildDetail?
      get() = metadata["buildDetail"] ?.let { objectMapper.convertValue<BuildDetail>(it) }
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
          debianArtifactSupplier.getLatestArtifact(deliveryConfig, artifact)
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
          verify(exactly = 0) { publisher.publishEvent(ofType<ArtifactVersionUpdated>())  }
        }
      }

      context("the version is new") {
        before {
          every {
            debianArtifactSupplier.getArtifactMetadata(newerPublishedDeb)
          } returns artifactMetadata

          every { repository.storeArtifactVersion(any()) } returns true
          every { repository.getAllArtifacts(DEBIAN, any()) } returns listOf(debianArtifact)

          subject.handlePublishedArtifact(newerPublishedDeb)
        }

        test("a new artifact version is stored") {
          val slot = slot<PublishedArtifact>()
          verify {repository.storeArtifactVersion(capture(artifactVersion)) }

          with(artifactVersion.captured) {
            expectThat(name).isEqualTo(artifact.name)
            expectThat(type).isEqualTo(artifact.type)
            expectThat(version).isEqualTo(newerPublishedDeb.version)
            expectThat(status).isEqualTo(ArtifactStatus.FINAL)
          }
        }

        test("artifact metadata is added before storing") {
          verify(exactly = 1) {
            debianArtifactSupplier.getArtifactMetadata(newerPublishedDeb)
          }

          with(artifactVersion.captured) {
            expectThat(gitMetadata).isEqualTo(artifactMetadata.gitMetadata)
            expectThat(buildMetadata).isEqualTo(artifactMetadata.buildMetadata)
          }
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
            buildService.getArtifactContents(buildController!!, buildJob!!, buildDetail!!.buildNumber,
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
          get { gitMetadata?.commit }.isEqualTo(disguisedPublishedDocker.buildDetail!!.commitId)
          get { buildMetadata?.id }.isEqualTo(disguisedPublishedDocker.buildDetail!!.buildNumber)
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
      subject.onApplicationUp()
    }

    context("events") {
      context("multiple artifacts") {
        before {
          every { repository.getAllArtifacts(DEBIAN, any()) } returns
            listOf(
              debianArtifact,
              debianArtifact.copy(reference = "blah-blay", deliveryConfigName = "another-config")
            )
          subject.notifyArtifactVersionDetected(publishedDeb)
        }

        test("publishes build lifecycle event for each artifact") {
          verify(exactly = 2) { publisher.publishEvent(ofType<LifecycleEvent>()) }
        }

        test("publishes version detected event for each artifact") {
          verify(exactly = 2) { publisher.publishEvent(ofType<ArtifactVersionDetected>()) }
        }
      }

      context("single artifact") {
        before {
          every { repository.getAllArtifacts(DEBIAN, any()) } returns
            listOf(debianArtifact)
          subject.notifyArtifactVersionDetected(publishedDeb)
        }

        test("publishes build lifecycle event with monitor = true") {
          val events = mutableListOf<Any>()
          verify(exactly = 2) { publisher.publishEvent(capture(events)) }
          expectThat(events).one {
            isA<LifecycleEvent>().and {
              get { status }.isEqualTo(LifecycleEventStatus.RUNNING)
              get { startMonitoring }.isTrue()
            }
          }
        }

        test("publishes artifact version detected event with the right information") {
          val events = mutableListOf<Any>()
          verify(exactly = 2) { publisher.publishEvent(capture(events)) }
          expectThat(events).one {
            isA<ArtifactVersionDetected>().and {
              get { deliveryConfig}.isEqualTo(deliveryConfig)
              get { artifact }.isEqualTo(debianArtifact)
              get { version }.isEqualTo(publishedDeb)
            }
          }
        }
      }
    }
  }
}
