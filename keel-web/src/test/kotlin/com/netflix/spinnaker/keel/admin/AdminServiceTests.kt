package com.netflix.spinnaker.keel.admin

import com.netflix.spinnaker.keel.actuation.ExecutionSummaryService
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier
import com.netflix.spinnaker.keel.application.ApplicationConfig
import com.netflix.spinnaker.keel.core.api.ManualJudgementConstraint
import com.netflix.spinnaker.keel.core.api.PipelineConstraint
import com.netflix.spinnaker.keel.core.api.PromotionStatus.CURRENT
import com.netflix.spinnaker.keel.core.api.TimeWindow
import com.netflix.spinnaker.keel.core.api.TimeWindowConstraint
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.Front50Service
import com.netflix.spinnaker.keel.front50.model.Application
import com.netflix.spinnaker.keel.front50.model.GenericStage
import com.netflix.spinnaker.keel.front50.model.ManagedDeliveryConfig
import com.netflix.spinnaker.keel.front50.model.Pipeline
import com.netflix.spinnaker.keel.front50.model.Trigger
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.persistence.ApplicationRepository
import com.netflix.spinnaker.keel.persistence.DiffFingerprintRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.test.DummyArtifact
import com.netflix.spinnaker.keel.test.DummySortingStrategy
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import org.springframework.context.ApplicationEventPublisher
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.isTrue
import io.mockk.coEvery as every
import io.mockk.coVerify as verify

class AdminServiceTests : JUnit5Minutests {
  class Fixture {
    val repository: KeelRepository = mockk(relaxed = true)
    val diffFingerprintRepository: DiffFingerprintRepository = mockk()
    val actuationPauser: ActuationPauser = mockk()
    private val artifactSupplier = mockk<ArtifactSupplier<DummyArtifact, DummySortingStrategy>>(relaxUnitFun = true)
    val front50Cache: Front50Cache = mockk()
    val clock = MutableClock()
    val applicationRepository: ApplicationRepository = mockk()

    val application = "fnord"

    val environment = Environment(
      name = "test",
      constraints = setOf(
        ManualJudgementConstraint(),
        PipelineConstraint(pipelineId = "wowapipeline"),
        TimeWindowConstraint(windows = listOf(TimeWindow(days = "Monday")))
      )
    )

    val artifactReference = "myartifact"

    val artifact = mockk<DeliveryArtifact> {
      every { reference } returns artifactReference
    }

    val deliveryConfig = DeliveryConfig(
      name = "manifest",
      application = application,
      serviceAccount = "keel@spinnaker",
      artifacts = setOf(artifact),
      environments = setOf(environment)
    )

    val front50Application = Application(
      name = deliveryConfig.name,
      email = "keel@keel.io",
      repoType = "stash",
      repoProjectKey = "proj",
      repoSlug = "repo",
      managedDelivery = ManagedDeliveryConfig(importDeliveryConfig = true, manifestPath = "spinnaker.yml")
    )

    val importPipeline = Pipeline(
      name = "Import",
      id = "config",
      application = front50Application.name,
      disabled = false,
      triggers = listOf(Trigger(type = "trigger", enabled = true, application = front50Application.name)),
      _stages = listOf(GenericStage(type = "importDeliveryConfig", name = "Import config", refId = "1"))
    )

    val executionSummaryService: ExecutionSummaryService = mockk()

    val subject = AdminService(
      repository,
      actuationPauser,
      diffFingerprintRepository,
      listOf(artifactSupplier),
      front50Cache,
      executionSummaryService,
      clock,
      applicationRepository
    )
  }

  fun adminServiceTests() = rootContext<Fixture> {
    fixture {
      Fixture()
    }

    before {
      every { repository.getDeliveryConfigForApplication(application) } returns deliveryConfig
    }

    context("forcing environment constraint reevaluation") {
      val version = "v0"
      test("clears state only for stateful constraints") {

        subject.forceConstraintReevaluation(application, environment.name, artifactReference, version)

        verify(exactly = 1) { repository.deleteConstraintState(deliveryConfig.name, environment.name,artifactReference, version, "manual-judgement") }
        verify(exactly = 1) { repository.deleteConstraintState(deliveryConfig.name, environment.name, artifactReference, version, "pipeline") }
        verify(exactly = 1) { repository.deleteConstraintState(deliveryConfig.name, environment.name, artifactReference, version, "allowed-times") }
      }

      test("clears a specific constraint type when asked to") {
        subject.forceConstraintReevaluation(application, environment.name, artifact.reference, version, "pipeline")

        verify(exactly = 0) { repository.deleteConstraintState(deliveryConfig.name, environment.name, artifactReference, version, "manual-judgement") }
        verify(exactly = 1) { repository.deleteConstraintState(deliveryConfig.name, environment.name, artifactReference, version, "pipeline") }
        verify(exactly = 0) { repository.deleteConstraintState(deliveryConfig.name, environment.name, artifactReference, version, "allowed-times") }
      }
    }

    test("forcing an artifact version to be skipped") {
      val current = mockk<PublishedArtifact> {
        every { reference } returns artifact.reference
        every { version } returns "v16"
      }

      every { repository.getArtifactVersionsByStatus(deliveryConfig, environment.name, any(), listOf(CURRENT)) } returns listOf(current)

      subject.forceSkipArtifactVersion(application, environment.name, artifact.reference, "v15")

      verify(exactly = 1) { repository.markAsSkipped(deliveryConfig, artifact, "v15", environment.name, "v16") }
    }

    context("refreshing the application cache") {
      before {
        every { front50Cache.primeCaches() } just runs
        subject.refreshApplicationCache()
      }

      test("delegates to the cache") {
        verify { front50Cache.primeCaches() }
      }
    }

    context("front50 config sync") {
      before {
        every {
          repository.allDeliveryConfigs(any())
        } returns setOf(deliveryConfig, deliveryConfig.copy(application = "fnord2"))

        every {
          front50Cache.applicationByName(any())
        } answers {
          front50Application.copy(name = firstArg<String>())
        }

        every {
          applicationRepository.store(any())
        } just runs

        subject.syncFront50Config()
      }

      test("Verify that we update all of the apps") {
        val appConfigs = mutableListOf<ApplicationConfig>()
        verify(exactly = 2) {
          applicationRepository.store(capture(appConfigs))
        }
        expectThat(appConfigs.map { it.application }).containsExactlyInAnyOrder("fnord", "fnord2")
      }
    }
  }
}
