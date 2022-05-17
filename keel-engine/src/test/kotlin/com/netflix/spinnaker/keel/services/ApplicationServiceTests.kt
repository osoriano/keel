package com.netflix.spinnaker.keel.services

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.keel.actuation.EnvironmentTaskCanceler
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.JiraBridge
import com.netflix.spinnaker.keel.api.ResourceStatus.CREATED
import com.netflix.spinnaker.keel.api.StashBridge
import com.netflix.spinnaker.keel.api.Verification
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.jira.JiraIssueResponse
import com.netflix.spinnaker.keel.api.migration.PrLink
import com.netflix.spinnaker.keel.core.api.ArtifactSummaryInEnvironment
import com.netflix.spinnaker.keel.core.api.DependsOnConstraint
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactPin
import com.netflix.spinnaker.keel.core.api.ManualJudgementConstraint
import com.netflix.spinnaker.keel.core.api.PipelineConstraint
import com.netflix.spinnaker.keel.core.api.ResourceAction.CREATE
import com.netflix.spinnaker.keel.core.api.ResourceAction.NONE
import com.netflix.spinnaker.keel.core.api.ResourceAction.UPDATE
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.core.api.SubmittedEnvironment
import com.netflix.spinnaker.keel.diff.DefaultResourceDiffFactory
import com.netflix.spinnaker.keel.events.PinnedNotification
import com.netflix.spinnaker.keel.events.UnpinnedNotification
import com.netflix.spinnaker.keel.exceptions.ValidationException
import com.netflix.spinnaker.keel.migrations.ApplicationPrData
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.pause.Pause
import com.netflix.spinnaker.keel.pause.PauseScope.RESOURCE
import com.netflix.spinnaker.keel.persistence.ApplicationPullRequestDataIsMissing
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.PausedRepository
import com.netflix.spinnaker.keel.scheduling.TemporalSchedulerService
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.serialization.configuredYamlMapper
import com.netflix.spinnaker.keel.test.DummyArtifact
import com.netflix.spinnaker.keel.test.DummyResourceHandlerV1
import com.netflix.spinnaker.keel.test.DummyResourceSpec
import com.netflix.spinnaker.keel.test.artifactReferenceResource
import com.netflix.spinnaker.keel.test.submittedResource
import com.netflix.spinnaker.keel.test.versionedArtifactResource
import com.netflix.spinnaker.keel.upsert.DeliveryConfigUpserter
import com.netflix.spinnaker.keel.validators.DeliveryConfigValidator
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.Runs
import io.mockk.coEvery as every
import io.mockk.coVerify as verify
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.spyk
import kotlinx.coroutines.runBlocking
import org.springframework.context.ApplicationEventPublisher
import retrofit.RetrofitError
import retrofit.client.Response
import strikt.api.Assertion.Builder
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isNotEmpty
import strikt.assertions.isSuccess
import strikt.assertions.isTrue
import strikt.assertions.second
import java.time.Instant
import java.time.ZoneId

class ApplicationServiceTests : JUnit5Minutests {
  class Fixture {
    val clock: MutableClock = MutableClock(
      Instant.parse("2020-03-25T00:00:00.00Z"),
      ZoneId.of("UTC")
    )
    val repository: KeelRepository = mockk {
      every {
        getVersionInfoInEnvironment(any(), any(), any())
      } returns emptyList()
    }
    val pausedRepository: PausedRepository = mockk()
    val resourceStatusService: ResourceStatusService = mockk()
    val deliveryConfigUpserter: DeliveryConfigUpserter = mockk()

    val application1 = "fnord1"
    val application2 = "fnord2"

    val releaseArtifact = DummyArtifact(reference = "release")
    val snapshotArtifact = DummyArtifact(reference = "snapshot")

    data class DummyVerification(override val id: String) : Verification {
      override val type = "dummy"
    }

    val singleArtifactEnvironments = listOf("test", "staging", "production").associateWith { name ->
      Environment(
        name = name,
        constraints = if (name == "production") {
          setOf(
            DependsOnConstraint("staging"),
            ManualJudgementConstraint(),
            PipelineConstraint(pipelineId = "fakePipeline")
          )
        } else {
          emptySet()
        },
        resources = setOf(
          // resource with new-style artifact reference
          artifactReferenceResource(artifactReference = "release"),
          // resource with old-style image provider
          versionedArtifactResource()
        ),
        verifyWith = when (name) {
          "test" -> listOf(DummyVerification("smoke"), DummyVerification("fuzz"))
          "staging" -> listOf(DummyVerification("end-to-end"), DummyVerification("canary"))
          else -> emptyList()
        }
      )
    }

    val singleArtifactDeliveryConfig = DeliveryConfig(
      name = "manifest_$application1",
      application = application1,
      serviceAccount = "keel@spinnaker",
      artifacts = setOf(releaseArtifact),
      environments = singleArtifactEnvironments.values.toSet()
    )

    val submittedDeliveryConfig = SubmittedDeliveryConfig(
      application = application1,
      name = "myconfig",
      serviceAccount = "keel@keel.io",
      artifacts = setOf(releaseArtifact),
      environments = setOf(
        SubmittedEnvironment(
          name = "test",
          resources = setOf(
            submittedResource()
          )
        )
      )
    )

    val dualArtifactEnvironments = listOf("pr", "test").associateWith { name ->
      Environment(
        name = name,
        constraints = emptySet(),
        resources = setOf(
          artifactReferenceResource(artifactReference = if (name == "pr") "snapshot" else "release")
        )
      )
    }

    val dualArtifactDeliveryConfig = DeliveryConfig(
      name = "manifest_$application2",
      application = application2,
      serviceAccount = "keel@spinnaker",
      artifacts = setOf(releaseArtifact, snapshotArtifact),
      environments = dualArtifactEnvironments.values.toSet()
    )

    val version0 = "fnord-1.0.0-h0.a0a0a0a"
    val version1 = "fnord-1.0.1-h1.b1b1b1b"
    val version2 = "fnord-1.0.2-h2.c2c2c2c"
    val version3 = "fnord-1.0.3-h3.d3d3d3d"
    val version4 = "fnord-1.0.4-h4.e4e4e4e"
    val version5 = "fnord-1.0.5-h5.f5f5f5f5"
    val versions = listOf(version0, version1, version2, version3, version4, version5)

    val pin = EnvironmentArtifactPin("production", releaseArtifact.reference, version0, "keel@keel.io", "comment")

    val publisher: ApplicationEventPublisher = mockk(relaxed = true)

    val springEnv: org.springframework.core.env.Environment = mockk {
      every {
        getProperty("keel.verifications.summary.enabled", Boolean::class.java, any())
      } returns true
    }

    val spectator = NoopRegistry()

    val environmentTaskCanceler: EnvironmentTaskCanceler = mockk(relaxUnitFun = true)
    val yamlMapper: YAMLMapper = configuredYamlMapper()
    val objectMapper = configuredObjectMapper().apply {
      registerSubtypes(
        NamedType(DummyArtifact::class.java, "dummy"),
        NamedType(DummyResourceSpec::class.java, "test/whatever@v1")
      )
    }
    val stashBridge: StashBridge = mockk(relaxed = true)
    val jiraBridge: JiraBridge = mockk(relaxed = true)
    val resourceHandler = spyk(DummyResourceHandlerV1)
    val diffFactory = DefaultResourceDiffFactory()
    val actuationPauser: ActuationPauser = mockk()
    val temporalSchedulerService: TemporalSchedulerService = mockk()
    val validator: DeliveryConfigValidator = mockk()

    // subject
    val applicationService = ApplicationService(
      repository,
      resourceStatusService,
      publisher,
      springEnv,
      clock,
      spectator,
      environmentTaskCanceler,
      yamlMapper,
      objectMapper,
      stashBridge,
      jiraBridge,
      pausedRepository,
      listOf(resourceHandler),
      diffFactory,
      deliveryConfigUpserter,
      actuationPauser,
      temporalSchedulerService,
      validator
    )

    val buildMetadata = BuildMetadata(
      id = 1,
      number = "1",
    )
  }

  fun applicationServiceTests() = rootContext<Fixture> {
    fixture {
      Fixture()
    }

    before {
      every { validator.validate(any()) } just Runs
      every { repository.getDeliveryConfigForApplication(application1) } returns singleArtifactDeliveryConfig
      every { repository.getDeliveryConfigForApplication(application2) } returns dualArtifactDeliveryConfig

      every {
        repository.getArtifactVersion(any(), any())
      } answers {
        PublishedArtifact(arg<DeliveryArtifact>(0).name, arg<DeliveryArtifact>(0).type, arg(1))
      }

      every {
        repository.getArtifactVersionByPromotionStatus(any(), any(), any(), any())
      } returns null

      every {
        repository.getPinnedVersion(any(), any(), any())
      } returns null

      every { repository.getArtifactSummariesInEnvironment(any(), any(), any(), any()) } returns emptyList()
      every { actuationPauser.getResourcePauseInfo(any()) } returns null
    }

    context("resource summaries by application") {
      before {
        every { resourceStatusService.getStatus(any()) } returns CREATED
      }

      test("includes all resources within the delivery config") {
        val summaries = applicationService.getResourceSummariesFor(application1)
        expectThat(summaries.size).isEqualTo(singleArtifactDeliveryConfig.resources.size)
      }

      test("sets the resource status as returned by ResourceStatusService") {
        val summaries = applicationService.getResourceSummariesFor(application1)
        expectThat(summaries.map { it.status }.all { it == CREATED }).isTrue()
      }

      context("resources are paused") {
        before {
          every { actuationPauser.getResourcePauseInfo(any()) } returns Pause(
            scope = RESOURCE,
            name = "what",
            pausedBy = "who",
            pausedAt = clock.instant(),
            comment = "why",
          )
        }

        test("pause reason is reflected") {
          val summaries = applicationService.getResourceSummariesFor(application1)
          expectThat(summaries.map { it.pause }.all { it != null }).isTrue()
        }
      }
    }

    context("pinning an artifact version in an environment") {
      before {
        every {
          repository.pinEnvironment(singleArtifactDeliveryConfig, pin)
        } just Runs
        every {
          repository.triggerDeliveryConfigRecheck(singleArtifactDeliveryConfig, any())
        } just Runs

        applicationService.pin("keel@keel.io", application1, pin)
      }


      test("causes the pin to be persisted") {
        verify(exactly = 1) {
          repository.pinEnvironment(singleArtifactDeliveryConfig, pin)
        }
      }

      test("pinned notification was sent") {
        verify { publisher.publishEvent(ofType<PinnedNotification>()) }
      }
    }

    context("unpinning a specific artifact in an environment") {
      before {
        every {
          repository.deletePin(singleArtifactDeliveryConfig, "production", releaseArtifact.reference)
        } just Runs

        every {
          repository.triggerDeliveryConfigRecheck(singleArtifactDeliveryConfig, any())
        } just Runs

        every {
          repository.pinnedEnvironments(singleArtifactDeliveryConfig)
        } returns emptyList()

        applicationService.deletePin("keel@keel.io", application1, "production", releaseArtifact.reference)
      }

      test("causes the pin to be deleted") {
        verify(exactly = 1) {
          repository.deletePin(singleArtifactDeliveryConfig, "production", releaseArtifact.reference)
        }
      }

      test("unpinned notification was sent") {
        verify { publisher.publishEvent(ofType<UnpinnedNotification>()) }
      }
    }

    context("unpinning all artifacts in an environment") {
      before {
        every {
          repository.deletePin(singleArtifactDeliveryConfig, "production")
        } just Runs

        every {
          repository.triggerDeliveryConfigRecheck(singleArtifactDeliveryConfig, any())
        } just Runs

        every {
          repository.pinnedEnvironments(singleArtifactDeliveryConfig)
        } returns emptyList()

        applicationService.deletePin("keel@keel.io", application1, "production")
      }

      test("causes all pins in the environment to be deleted") {
        verify(exactly = 1) {
          repository.deletePin(singleArtifactDeliveryConfig, "production")
        }
      }

      test("slack unpinned event was sent") {
        verify { publisher.publishEvent(ofType<UnpinnedNotification>()) }
      }
    }

    context("open PR with a config successfully") {
      val expectedPrResponse = PrLink(link = "https://stash/users/bla/repos/bla-app/pull-requests/93")
      before {
        every { repository.getMigratableApplicationData(application1) } returns ApplicationPrData(
          submittedDeliveryConfig,
          null,
          "repo",
          "project"
        )

        every {
          repository.storePrLinkForMigratedApplication(application1, any())
        } just Runs

        every {
          stashBridge.createCommitAndPrFromConfig(any())
        } returns expectedPrResponse.link

        every {
          repository.storeUserGeneratedConfigForMigratedApplication(application1, any(), any())
        } just Runs
      }

      test("successfully created a PR in stash with the config; not failed when jira integration is returning error") {
        expectCatching {
          applicationService.openMigrationPr(application1, "keel")
        }.isSuccess()
          .second.isEqualTo(expectedPrResponse.link)
      }

      test("successfully created a PR in stash with the config for submitted config") {
        expectCatching {
          applicationService.openMigrationPr(application1, "keel", submittedDeliveryConfig)
        }.isSuccess()
          .second.isEqualTo(expectedPrResponse.link)
      }

      context("Invalid config") {
        before {
          every {
            validator.validate(any())
          }.throws(ValidationException("bad config"))
        }

        test("PR is not created") {
          expectCatching {
            applicationService.openMigrationPr(application1, "keel", submittedDeliveryConfig)
          }.isFailure()
        }
      }

      context("with jira") {
        before {
          every {
            jiraBridge.createIssue(any())
          } returns JiraIssueResponse("123", "MD-1", "jira/MD-1")

          every {
            repository.storeJiraLinkForMigratedApplication(application1, "jira/MD-1")
          } just Runs
        }

        test("successfully created a PR in stash with the config; create a jira issue") {
          runBlocking {
            applicationService.openMigrationPr(application1, "keel")
          }
          verify(exactly = 1) {
            repository.storeJiraLinkForMigratedApplication(application1, "jira/MD-1")
          }
        }
      }

    }

    context("errors when creating from stash") {
      val retrofitError = RetrofitError.httpError(
        "http://stash",
        Response("http://stash", 409, "duplicate", emptyList(), null),
        null, null
      )

      before {
        every {
          stashBridge.createCommitAndPrFromConfig(any())
        } throws retrofitError

        every { repository.getMigratableApplicationData(application1) } returns ApplicationPrData(
          submittedDeliveryConfig,
          null,
          "repo",
          "project"
        )
      }

      test("bubbles up http errors when trying to create a PR") {
        expectCatching {
          applicationService.openMigrationPr(application1, "keel")
        }
          .isFailure()
          .isEqualTo(retrofitError)
      }
    }

    context("application PR data does not exists") {
      val exception = ApplicationPullRequestDataIsMissing(application = "fnord1")
      before {
        every { repository.getMigratableApplicationData(application1) } throws exception
      }
      test("throw an exception") {
        expectCatching {
          applicationService.openMigrationPr(application1, "keel")
        }
          .isFailure()
          .isA<ApplicationPullRequestDataIsMissing>()
      }
    }

    context("store a paused migration config") {
      before {
        every {
          repository.getMigratableApplicationData(application1)
        } returns ApplicationPrData(submittedDeliveryConfig, null, "repo", "project")

        every {
          deliveryConfigUpserter.upsertConfig(any(), any(), any(), any())
        } returns Pair(submittedDeliveryConfig.toDeliveryConfig(), true)

        every { pausedRepository.pauseApplication(any(), any(), any()) } just runs
      }

      test("application is paused and delivery config is stored") {
        expectCatching {
          applicationService.storePausedMigrationConfig(application1, "keel", submittedDeliveryConfig)
        }.isSuccess()

        verify {
          pausedRepository.pauseApplication(application1, "keel", any())
          deliveryConfigUpserter.upsertConfig(submittedDeliveryConfig, any(), true, any())
        }
      }
    }

    context("getting an actuation plan for an application") {
      before {
        every {
          repository.getDeliveryConfigForApplication(application1)
        } returns submittedDeliveryConfig.toDeliveryConfig()

        every {
          resourceHandler.desired(any())
        } returns DummyResourceSpec(id = "id", data = "desired")
      }

      context("when resource does not exist") {
        before {
          every {
            resourceHandler.current(any())
          } returns null
        }

        test("indicates CREATE action in the plan") {
          expectCatching {
            applicationService.getActuationPlan(application1)
          }.isSuccess()
            .get { environmentPlans.first().resourcePlans.first() }
            .and {
              get { diff }.isNotEmpty()
              get { action }.isEqualTo(CREATE)
            }
        }
      }

      context("when desired state is equal to current") {
        before {
          every {
            resourceHandler.current(any())
          } returns DummyResourceSpec(id = "id", data = "desired")
        }

        test("indicates no action in the plan") {
          expectCatching {
            applicationService.getActuationPlan(application1)
          }.isSuccess()
            .get { environmentPlans.first().resourcePlans.first() }
            .and {
              get { diff }.isEmpty()
              get { action }.isEqualTo(NONE)
            }
        }
      }

      context("when desired state is different from current") {
        before {
          every {
            resourceHandler.current(any())
          } returns DummyResourceSpec(id = "id", data = "current")
        }

        test("indicates UPDATE action in the plan") {
          expectCatching {
            applicationService.getActuationPlan(application1)
          }.isSuccess()
            .get { environmentPlans.first().resourcePlans.first() }
            .and {
              get { diff }.isNotEmpty()
              get { action }.isEqualTo(UPDATE)
            }
        }
      }
    }

    context("deleting the config") {
      before {
        every { repository.getDeliveryConfigForApplication(application1) } returns singleArtifactDeliveryConfig
        every { repository.getDeliveryConfig(singleArtifactDeliveryConfig.name) } returns singleArtifactDeliveryConfig
        every { repository.deleteDeliveryConfigByName(singleArtifactDeliveryConfig.name) } just Runs
        every { repository.deleteDeliveryConfigByApplication(application1) } just Runs
        every { temporalSchedulerService.stopScheduling(any()) } just Runs
        every { temporalSchedulerService.stopScheduling(any(), any()) } just Runs
      }

      test("deleting the config by app also calls stop scheduling of all resources") {
        applicationService.deleteConfigByApp(application1)
        Thread.sleep(100)
        verify(exactly = 6) { temporalSchedulerService.stopScheduling(any()) }
      }

      test("deleting the config by name also calls stop scheduling of all resources and envs") {
        applicationService.deleteDeliveryConfig(singleArtifactDeliveryConfig.name)
        verify(exactly = 6) { temporalSchedulerService.stopScheduling(any()) }
        verify(exactly = 3) { temporalSchedulerService.stopScheduling(any(), any()) }
      }
    }
  }

  val Builder<ArtifactSummaryInEnvironment>.state: Builder<String>
    get() = get { state }
}
