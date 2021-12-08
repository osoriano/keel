package com.netflix.spinnaker.keel.scm

import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Tag
import com.netflix.spinnaker.keel.api.artifacts.ArtifactOriginFilter
import com.netflix.spinnaker.keel.api.artifacts.branchName
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.auth.AuthorizationResourceType.SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.auth.AuthorizationSupport
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.core.api.SubmittedEnvironment
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.model.Application
import com.netflix.spinnaker.keel.front50.model.DataSources
import com.netflix.spinnaker.keel.front50.model.ManagedDeliveryConfig
import com.netflix.spinnaker.keel.igor.DeliveryConfigImporter
import com.netflix.spinnaker.keel.notifications.DeliveryConfigImportFailed
import com.netflix.spinnaker.keel.persistence.DismissibleNotificationRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.scm.DeliveryConfigCodeEventListener.Companion.CODE_EVENT_COUNTER
import com.netflix.spinnaker.keel.test.submittedResource
import com.netflix.spinnaker.keel.upsert.DeliveryConfigUpserter
import com.netflix.spinnaker.kork.exceptions.SystemException
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.TestContextBuilder
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.called
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import org.springframework.context.ApplicationEventPublisher
import org.springframework.core.env.Environment
import org.springframework.security.access.AccessDeniedException
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.isEqualTo
import strikt.assertions.one
import io.mockk.coEvery as every
import io.mockk.coVerify as verify

class DeliveryConfigCodeEventListenerTests : JUnit5Minutests {
  class Fixture {
    val keelRepository: KeelRepository = mockk()
    val deliveryConfigUpserter: DeliveryConfigUpserter = mockk()
    val importer: DeliveryConfigImporter = mockk()
    val front50Cache: Front50Cache = mockk()
    val scmUtils: ScmUtils = mockk()
    val springEnv: Environment = mockk()
    val notificationRepository: DismissibleNotificationRepository = mockk()
    val spectator: Registry = mockk()
    val clock = MutableClock()
    val eventPublisher: ApplicationEventPublisher = mockk(relaxUnitFun = true)
    val authorizationSupport: AuthorizationSupport = mockk(relaxUnitFun = true)
    val subject = DeliveryConfigCodeEventListener(
      keelRepository = keelRepository,
      deliveryConfigUpserter = deliveryConfigUpserter,
      deliveryConfigImporter = importer,
      notificationRepository = notificationRepository,
      front50Cache = front50Cache,
      scmUtils = scmUtils,
      springEnv = springEnv,
      spectator = spectator,
      eventPublisher = eventPublisher,
      authorizationSupport = authorizationSupport,
      clock = clock
    )

    val configuredApp = Application(
      name = "fnord",
      email = "keel@keel.io",
      repoType = "stash",
      repoProjectKey = "myorg",
      repoSlug = "myrepo",
      managedDelivery = ManagedDeliveryConfig(importDeliveryConfig = true),
      dataSources = DataSources(enabled = emptyList(), disabled = emptyList())
    )

    val notConfiguredApp = Application(
      name = "notfnord",
      email = "keel@keel.io",
      repoType = "stash",
      repoProjectKey = "myorg",
      repoSlug = "another-repo",
      dataSources = DataSources(enabled = emptyList(), disabled = emptyList())
    )

    val migratingApp = configuredApp.copy(name = "migratingfnord")

    val artifactFromMain = DockerArtifact(
      name = "myorg/myartifact",
      deliveryConfigName = "myconfig",
      reference = "myartifact-main",
      from = ArtifactOriginFilter(branch = branchName("main"))
    )

    val deliveryConfig = SubmittedDeliveryConfig(
      application = "fnord",
      name = "myconfig",
      serviceAccount = "keel@keel.io",
      artifacts = setOf(artifactFromMain),
      environments = setOf(
        SubmittedEnvironment(
          name = "test",
          resources = setOf(
            submittedResource()
          )
        )
      )
    )

    fun setupMocks() {
      every {
        springEnv.getProperty("keel.importDeliveryConfigs.enabled", Boolean::class.java, true)
      } returns true

      every {
        spectator.counter(any(), any<Iterable<Tag>>())
      } returns mockk {
        every {
          increment()
        } just runs
      }

      every {
        front50Cache.searchApplicationsByRepo(any())
      } returns listOf(configuredApp, notConfiguredApp)

      every {
        importer.import(any<CodeEvent>(), any())
      } returns deliveryConfig

      every {
        deliveryConfigUpserter.upsertConfig(deliveryConfig, any())
      } returns Pair(deliveryConfig.toDeliveryConfig(), false)

      every {
        scmUtils.getDefaultBranch(any())
      } returns "main"

      every {
        notificationRepository.dismissNotification(any<Class<DeliveryConfigImportFailed>>(), any(), any(), any())
      } returns true

      every {
        scmUtils.getCommitLink(any())
      } returns "https://commit-link.org"

      every {
        keelRepository.isApplicationConfigured(any())
      } answers {
        firstArg<String>() == deliveryConfig.application
      }

      every {
        front50Cache.updateManagedDeliveryConfig(any<Application>(), any(), any())
      } answers {
        firstArg()
      }

      every {
        keelRepository.isMigrationPr(any(), any())
      } answers {
        firstArg<String>() == migratingApp.name && secondArg<String>() == "23"
      }
    }
  }

  val commitEvent = CommitCreatedEvent(
    repoKey = "stash/myorg/myrepo",
    targetBranch = "main",
    commitHash = "1d52038730f431be19a8012f6f3f333e95a53772",
    authorEmail = "author@keel.io",
    causeByEmail = "joe@keel.io"
  )

  val prMergedEvent = PrMergedEvent(
    repoKey = "stash/myorg/myrepo",
    targetBranch = "main",
    commitHash = "1d52038730f431be19a8012f6f3f333e95a53772",
    pullRequestBranch = "pr1",
    pullRequestId = "23",
    authorEmail = "author@keel.io",
    causeByEmail = "joe@keel.io"
  )

  val commitEventForAnotherBranch = commitEvent.copy(targetBranch = "not-a-match")

  // matches repo for nonConfiguredApp
  val commitEventForAnotherRepo = commitEvent.copy(repoKey = "stash/myorg/another-repo")

  fun tests() = rootContext<Fixture> {
    fixture { Fixture() }

    context("an application is configured to retrieve the delivery config from source") {
      before {
        setupMocks()
      }

      listOf(commitEvent, prMergedEvent).map { event ->
        context("a code event matching the repo and branch is received") {
          before {
            subject.handleCodeEvent(event)
          }

          test("the delivery config is imported from the commit in the event") {
            verify(exactly = 1) {
              importer.import(
                codeEvent = event,
                manifestPath = any()
              )
            }
          }

          test("access of the code change author to the service account in the delivery config is checked") {
            verify {
              authorizationSupport.checkPermission(event.causeByEmail!!, deliveryConfig.serviceAccount!!, SERVICE_ACCOUNT, "ACCESS")
            }
          }

          test("the delivery config is created/updated") {
            verify {
              deliveryConfigUpserter.upsertConfig(deliveryConfig, any())
            }
          }

          test("notification was dismissed on successful import") {
            verify {
              notificationRepository.dismissNotification(
                any<Class<DeliveryConfigImportFailed>>(),
                deliveryConfig.application,
                event.targetBranch,
                any()
              )
            }
          }

          test("auto import is not enabled for an existing app") {
            verify(exactly = 0) {
              front50Cache.updateManagedDeliveryConfig(any<Application>(), any(), any())
            }
          }

          test("a successful delivery config retrieval is counted") {
            val tags = mutableListOf<Iterable<Tag>>()
            verify {
              spectator.counter(CODE_EVENT_COUNTER, capture(tags))
            }
            expectThat(tags).one {
              contains(DELIVERY_CONFIG_RETRIEVAL_SUCCESS.toTags())
            }
          }
        }
      }

      context("fallback to author email if cause by is missing") {
        before {
          subject.handleCodeEvent(prMergedEvent.copy(causeByEmail = null))
        }
        test("use author email") {
          verify {
            authorizationSupport.checkPermission(prMergedEvent.authorEmail!!, deliveryConfig.serviceAccount!!, SERVICE_ACCOUNT, "ACCESS")
          }
        }
      }

      context("apps that are not on Managed Delivery yet") {
        before {
          every {
            front50Cache.searchApplicationsByRepo(any())
          } returns listOf(configuredApp.copy(name = "notConfiguredApp"))

          subject.handleCodeEvent(commitEvent)
        }

        verifyEventIgnored()
      }

      context("onboarding a new app") {
        before {
          every {
            front50Cache.searchApplicationsByRepo(any())
          } returns listOf(migratingApp)

          every {
            deliveryConfigUpserter.upsertConfig(deliveryConfig, any())
          } returns Pair(deliveryConfig.toDeliveryConfig(), true)
        }

        test("config is upserted for a new app") {
          subject.handleCodeEvent(prMergedEvent)

          verify {
            deliveryConfigUpserter.upsertConfig(deliveryConfig, any())
          }

          verify {
            front50Cache.updateManagedDeliveryConfig(migratingApp, any(), any())
          }
        }

        test("ignoring non-matching PRs") {
          subject.handleCodeEvent(prMergedEvent.copy(pullRequestId = "25"))
          verifyEventIgnored()
        }
      }

      context("apps with custom manifest path") {
        val manifestPath = "custom/spinnaker.yml"
        before {
          every {
            front50Cache.searchApplicationsByRepo(any())
          } returns listOf(
            configuredApp.copy(
              managedDelivery = ManagedDeliveryConfig(
                importDeliveryConfig = true,
                manifestPath = manifestPath
              )
            ),
            notConfiguredApp
          )
        }

        test("importing the manifest from the correct path") {
          subject.handleCodeEvent(commitEvent)
          verify(exactly = 1) {
            importer.import(
              codeEvent = commitEvent,
              manifestPath = manifestPath
            )
          }
        }
      }

      context("a commit event NOT matching the app repo is received") {
        before {
          subject.handleCodeEvent(commitEventForAnotherRepo)
        }

        verifyEventIgnored()
      }

      context("a commit event NOT matching the app default branch is received") {
        before {
          subject.handleCodeEvent(commitEventForAnotherBranch)
        }

        verifyEventIgnored()
      }
    }

    context("an application is NOT configured to retrieve the delivery config from source") {
      before {
        setupMocks()
      }

      context("a commit event matching the repo and branch is received") {
        before {
          subject.handleCodeEvent(commitEventForAnotherRepo)
        }

        verifyEventIgnored()
      }
    }

    context("feature flag is disabled") {
      before {
        setupMocks()
      }

      context("a commit event matching the repo and branch is received") {
        modifyFixture {
          every {
            springEnv.getProperty("keel.importDeliveryConfigs.enabled", Boolean::class.java, true)
          } returns false
        }

        before {
          subject.handleCodeEvent(commitEventForAnotherRepo)
        }

        verifyEventIgnored()
      }
    }

    context("error scenarios") {
      before {
        setupMocks()
      }

      listOf(commitEvent, prMergedEvent).forEach { event ->
        context("failure to retrieve delivery config for $event") {
          modifyFixture {
            every {
              importer.import(event, manifestPath = any())
            } throws SystemException("oh noes!")
          }

          before {
            subject.handleCodeEvent(event)
          }

          verifyErrorMetricIncreased()
          verifyErrorEventEmitted(event)
        }

        context("code event author is not authorized to access service account for $event") {
          modifyFixture {
            every {
              authorizationSupport.checkPermission(any<String>(), any(), any(), any())
            } throws AccessDeniedException("you shall not pass!")
          }

          before {
            subject.handleCodeEvent(event)
          }

          test("access denied exception is handled") {
            // no-op, just proves we get here
          }

          verifyErrorMetricIncreased()
          verifyErrorEventEmitted(event, false)
        }
      }
    }
  }

  private fun TestContextBuilder<Fixture, Fixture>.verifyErrorMetricIncreased() {
    test("a delivery config retrieval error is counted") {
      val tags = mutableListOf<Iterable<Tag>>()
      verify {
        spectator.counter(CODE_EVENT_COUNTER, capture(tags))
      }
      expectThat(tags).one {
        contains(DELIVERY_CONFIG_RETRIEVAL_ERROR.toTags())
      }
    }
  }

  private fun TestContextBuilder<Fixture, Fixture>.verifyErrorEventEmitted(event: CodeEvent, checkEmitted: Boolean = true) {
    if (checkEmitted) {
      test("an error event is published") {
        val failureEvent = slot<DeliveryConfigImportFailed>()
        verify {
          eventPublisher.publishEvent(capture(failureEvent))
        }
        expectThat(failureEvent.captured.branch).isEqualTo(event.targetBranch)
      }
    } else {
      test("an error event is not published") {
        verify(exactly = 0) {
          eventPublisher.publishEvent(ofType<DeliveryConfigImportFailed>())
        }
      }
    }
  }

  private fun TestContextBuilder<Fixture, Fixture>.verifyEventIgnored() {
    test("the event is ignored") {
      verify {
        importer wasNot called
      }
      verify {
        deliveryConfigUpserter wasNot called
      }
    }
  }
}
