package com.netflix.spinnaker.keel.sql

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.config.PersistenceRetryConfig
import com.netflix.spinnaker.config.ResourceEventPruneConfig
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.NotificationConfig
import com.netflix.spinnaker.keel.api.NotificationFrequency.normal
import com.netflix.spinnaker.keel.api.NotificationType.slack
import com.netflix.spinnaker.keel.api.PreviewEnvironmentSpec
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.Verification
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.BRANCH_JOB_COMMIT_BY_JOB
import com.netflix.spinnaker.keel.api.artifacts.branchStartsWith
import com.netflix.spinnaker.keel.api.artifacts.from
import com.netflix.spinnaker.keel.api.constraints.ConstraintState
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.FAIL
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.OVERRIDE_FAIL
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PENDING
import com.netflix.spinnaker.keel.api.events.ArtifactRegisteredEvent
import com.netflix.spinnaker.keel.api.events.ConstraintStateChanged
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.core.api.MANUAL_JUDGEMENT_CONSTRAINT_TYPE
import com.netflix.spinnaker.keel.core.api.ManualJudgementConstraint
import com.netflix.spinnaker.keel.core.api.SubmittedResource
import com.netflix.spinnaker.keel.core.api.normalize
import com.netflix.spinnaker.keel.diff.DefaultResourceDiffFactory
import com.netflix.spinnaker.keel.events.ResourceCreated
import com.netflix.spinnaker.keel.events.ResourceState.Ok
import com.netflix.spinnaker.keel.events.ResourceUpdated
import com.netflix.spinnaker.keel.exceptions.DuplicateManagedResourceException
import com.netflix.spinnaker.keel.persistence.ArtifactNotFoundException
import com.netflix.spinnaker.keel.persistence.ConflictingDeliveryConfigsException
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.NoSuchDeliveryConfigException
import com.netflix.spinnaker.keel.persistence.NoSuchResourceException
import com.netflix.spinnaker.keel.persistence.PersistenceRetry
import com.netflix.spinnaker.keel.persistence.TooManyDeliveryConfigsException
import com.netflix.spinnaker.keel.resources.ResourceFactory
import com.netflix.spinnaker.keel.test.DummyResourceSpec
import com.netflix.spinnaker.keel.test.TEST_API_V1
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import com.netflix.spinnaker.keel.test.defaultArtifactSuppliers
import com.netflix.spinnaker.keel.test.mockEnvironment
import com.netflix.spinnaker.keel.test.resource
import com.netflix.spinnaker.keel.test.resourceFactory
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.springframework.context.ApplicationEventPublisher
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.first
import strikt.assertions.hasSize
import strikt.assertions.isA
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEmpty
import strikt.assertions.isNotNull
import strikt.assertions.isSuccess
import strikt.assertions.isTrue
import java.time.Clock
import java.time.Duration

/**
 * Tests that involve creating, updating, or deleting things from two or more of the three repositories present.
 *
 * Tests that only apply to one repository should live in the repository-specific test classes.
 */
class SqlKeelRepositoryTests : JUnit5Minutests {

  private val jooq = testDatabase.context
  private val objectMapper = configuredTestObjectMapper().apply {
    registerSubtypes(NamedType(ManualJudgementConstraint::class.java, MANUAL_JUDGEMENT_CONSTRAINT_TYPE))
    registerSubtypes(NamedType(DummyVerification::class.java, DummyVerification.TYPE))
  }
  private val retryProperties = RetryProperties(1, 0)
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties))
  private val clock = Clock.systemUTC()

  private fun createDeliveryConfigRepository(resourceFactory: ResourceFactory): SqlDeliveryConfigRepository =
    SqlDeliveryConfigRepository(
        jooq,
        clock,
        objectMapper,
        resourceFactory,
        sqlRetry,
        defaultArtifactSuppliers(),
        publisher = mockk(relaxed = true),
        featureToggles = mockk()
    )

  private fun createResourceRepository(resourceFactory: ResourceFactory): SqlResourceRepository =
    SqlResourceRepository(jooq, clock, objectMapper, resourceFactory, sqlRetry, publisher = mockk(relaxed = true), spectator = NoopRegistry(), springEnv = mockEnvironment(), resourceEventPruneConfig = ResourceEventPruneConfig())

  private fun createArtifactRepository(): SqlArtifactRepository =
    SqlArtifactRepository(jooq, clock, objectMapper, sqlRetry, defaultArtifactSuppliers(), publisher = mockk(relaxed = true))

  private fun createVerificationRepository(resourceFactory: ResourceFactory): SqlActionRepository =
    SqlActionRepository(jooq, clock, objectMapper, resourceFactory, sqlRetry, environment = mockk())

  private fun createNotificationRepository(): SqlNotificationRepository =
    SqlNotificationRepository(jooq, clock, sqlRetry)

  private fun flush() {
    SqlTestUtil.cleanupDb(jooq)
  }

  data class DummyVerification(val label: String) : Verification {
    override val type = TYPE
    override val id: String
      get() = label

    companion object {
      const val TYPE = "dummy"
    }
  }

  val configName = "my-config"
  val secondConfigName = "my-config-2"
  val application = "fnord"
  val secondApplication = "fnord-2"
  val artifact = DockerArtifact(name = "org/image", deliveryConfigName = configName, branch = "main")
  val newArtifact = artifact.copy(reference = "myart")
  val firstResource = resource()
  val secondResource = resource()

  val verification = DummyVerification("1")
  val secondVerification = DummyVerification("2")
  val firstEnv = Environment(name = "env1", resources = setOf(firstResource), verifyWith = listOf(verification))
  val secondEnv = Environment(name = "env2", resources = setOf(secondResource))
  val previewEnv = PreviewEnvironmentSpec(
    branch = branchStartsWith("feature/"),
    baseEnvironment = "env1"
  )
  val deliveryConfig = DeliveryConfig(
    name = configName,
    application = application,
    serviceAccount = "keel@spinnaker",
    artifacts = setOf(artifact),
    environments = setOf(firstEnv),
    previewEnvironments = setOf(previewEnv)
  )

  val secondDeliveryConfig = DeliveryConfig(
    name = secondConfigName,
    application = secondApplication,
    serviceAccount = "keel@spinnaker",
    artifacts = setOf(artifact),
    environments = setOf(firstEnv)
  )

  val anotherDeliveryConfigWithSameName = DeliveryConfig(
    name = configName,
    application = secondApplication,
    serviceAccount = "keel@spinnaker",
    artifacts = setOf(),
    environments = setOf()
  )

  val anotherDeliveryConfigWithSameApp = DeliveryConfig(
    name = secondConfigName,
    application = application,
    serviceAccount = "keel@spinnaker",
    artifacts = setOf(artifact),
    environments = setOf(firstEnv)
  )

  val deliveryConfigWithVerificationRemoved = deliveryConfig.copy(
    environments = setOf(firstEnv.copy(verifyWith=emptyList()))
  )

  val configWithStatefulConstraint = deliveryConfig.copy(environments = setOf(firstEnv.copy(constraints = setOf(ManualJudgementConstraint()))))

  data class Fixture(
    val deliveryConfigRepositoryProvider: (ResourceFactory) -> SqlDeliveryConfigRepository,
    val resourceRepositoryProvider: (ResourceFactory) -> SqlResourceRepository,
    val artifactRepositoryProvider: () -> SqlArtifactRepository,
    val notificationRepositoryProvider: () -> SqlNotificationRepository,
    val actionRepositoryProvider: (ResourceFactory) -> SqlActionRepository
  ) {
    internal val clock = MutableClock()
    val publisher: ApplicationEventPublisher = mockk(relaxUnitFun = true)
    private val dummyResourceFactory = resourceFactory()
    internal val deliveryConfigRepository = deliveryConfigRepositoryProvider(dummyResourceFactory)
    internal val resourceRepository = spyk(resourceRepositoryProvider(dummyResourceFactory))
    internal val artifactRepository= artifactRepositoryProvider()
    internal val actionRepository = actionRepositoryProvider(dummyResourceFactory)
    internal val notificationRepository = notificationRepositoryProvider()

    val subject = KeelRepository(
      deliveryConfigRepository,
      artifactRepository,
      resourceRepository,
      actionRepository,
      clock,
      publisher,
      DefaultResourceDiffFactory(),
      PersistenceRetry(PersistenceRetryConfig()),
      notificationRepository
    )

    fun resourcesDueForCheck() =
      subject.resourcesDueForCheck(Duration.ofMinutes(1), Int.MAX_VALUE)
        .onEach { subject.markResourceCheckComplete(it, Ok) }

    fun KeelRepository.allResourceNames(): List<String> =
      mutableListOf<String>()
        .also { list ->
          allResources { list.add(it.id) }
        }
  }

  fun tests() = rootContext<Fixture> {
    fixture {
      Fixture(
        deliveryConfigRepositoryProvider = this@SqlKeelRepositoryTests::createDeliveryConfigRepository,
        resourceRepositoryProvider = this@SqlKeelRepositoryTests::createResourceRepository,
        artifactRepositoryProvider = this@SqlKeelRepositoryTests::createArtifactRepository,
        actionRepositoryProvider = this@SqlKeelRepositoryTests::createVerificationRepository,
        notificationRepositoryProvider = this@SqlKeelRepositoryTests::createNotificationRepository
      )
    }

    after {
      flush()
    }

    context("creating and deleting delivery configs") {
      before {
        subject.upsertDeliveryConfig(deliveryConfig)
      }

      context("delivery config created") {
        test("delivery config is persisted") {
          expectCatching { subject.getDeliveryConfig(deliveryConfig.name) }
            .isSuccess()
        }

        test("artifacts are persisted") {
          expectThat(subject.isRegistered("org/image", DOCKER)).isTrue()
          verify {
            publisher.publishEvent(
              ArtifactRegisteredEvent(
                DockerArtifact(
                  name = "org/image",
                  deliveryConfigName = configName,
                  branch = "main"
                )
              )
            )
          }
        }

        test("individual resources are persisted") {
          deliveryConfig.resources.map { it.id }.forEach { id ->
            expectCatching {
              subject.getResource(id)
            }.isSuccess()
          }
        }

        test("records that each resource was created") {
          verify(exactly = deliveryConfig.resources.size) {
            resourceRepository.appendHistory(ofType<ResourceCreated>())
          }
        }
      }

      context("delivery config was deleted") {
        before {
          subject.deleteDeliveryConfigByApplication(deliveryConfig.application)
        }
        test("everything is deleted") {
          expectThrows<NoSuchDeliveryConfigException> { deliveryConfigRepository.get(configName) }
          expectThrows<NoSuchResourceException> { resourceRepository.get(firstResource.id) }
          expectThat(artifactRepository.get(artifact.name, artifact.type, configName)).isEmpty()
        }
      }

      context("delivery config is updated") {
        context("artifact and resource have changed") {
          before {
            val updatedConfig = deliveryConfig.copy(
              artifacts = setOf(newArtifact),
              environments = setOf(firstEnv.copy(resources = setOf(secondResource)))
            )
            subject.upsertDeliveryConfig(updatedConfig)
          }

          test("no longer present dependents are removed") {
            expectThrows<NoSuchResourceException> { resourceRepository.get(firstResource.id) }
            expectThrows<ArtifactNotFoundException> {
              artifactRepository.get(
                name = artifact.name,
                type = artifact.type,
                reference = "org/image",
                deliveryConfigName = configName
              )
            }
          }

          test("correct resources still exist") {
            expectCatching { resourceRepository.get(secondResource.id) }.isSuccess()
            expectCatching {
              artifactRepository.get(
                name = newArtifact.name,
                type = newArtifact.type,
                reference = "myart",
                deliveryConfigName = configName
              )
            }.isSuccess()
          }
        }

        context("artifact properties modified") {
          before {
            val updatedConfig = deliveryConfig.copy(
              artifacts = setOf(artifact.copy(from = null, tagVersionStrategy = BRANCH_JOB_COMMIT_BY_JOB))
            )
            subject.upsertDeliveryConfig(updatedConfig)
          }

          test("artifact is updated but still present") {
            expectCatching {
              artifactRepository.get(
                name = artifact.name,
                type = artifact.type,
                reference = artifact.reference,
                deliveryConfigName = configName
              )
            }.isSuccess()
              .isA<DockerArtifact>()
              .get { tagVersionStrategy }.isEqualTo(BRANCH_JOB_COMMIT_BY_JOB)
          }
        }

        context("environment changed and artifact removed") {
          before {
            val updatedConfig = deliveryConfig.copy(
              artifacts = setOf(),
              environments = setOf(firstEnv.copy(name = "env2"))
            )
            subject.upsertDeliveryConfig(updatedConfig)
          }
          test("old environment is gone") {
            val config = deliveryConfigRepository.get(configName)
            expect {
              that(config.environments.size).isEqualTo(1)
              that(config.environments.first().name).isEqualTo("env2")
              that(config.resources.size).isEqualTo(1)
              that(config.resources.first().id).isEqualTo(firstResource.id)
              that(config.artifacts.size).isEqualTo(0)
              that(artifactRepository.get(artifact.name, artifact.type, configName)).isEmpty()
            }
          }
        }

        context("preview environment config removed") {
          before {
            val updatedConfig = deliveryConfig.copy(
              previewEnvironments = emptySet()
            )
            subject.upsertDeliveryConfig(updatedConfig)
          }
          test("old preview environment is gone") {
            val config = deliveryConfigRepository.get(configName)
            expect {
              that(config.previewEnvironments).isEmpty()
            }
          }
        }

        context("preview environment config changed") {
          before {
            val updatedConfig = deliveryConfig.copy(
              previewEnvironments = setOf(
                previewEnv.copy(notifications = setOf(NotificationConfig(slack, "#test", normal)))
              )
            )
            subject.upsertDeliveryConfig(updatedConfig)
          }
          test("preview environment is modified accordingly") {
            val config = deliveryConfigRepository.get(configName)
            expect {
              that(config.previewEnvironments).hasSize(1)
              that(config.previewEnvironments.first().notifications).isNotEmpty()
            }
          }
        }

        context("preview environments and artifacts exist in the database") {
          val previewArtifact = DockerArtifact("myimage", configName, from = from(branchStartsWith("feature/")), isPreview = true)
          val previewEnvironment = Environment("preview", isPreview = true, resources = setOf(resource()))

          before {
            val updatedConfig = deliveryConfig.run {
              copy(
                artifacts = artifacts + previewArtifact,
                environments = environments + previewEnvironment
              )
            }
            subject.upsertDeliveryConfig(updatedConfig)
          }

          context("incoming config does not include preview objects (e.g. from git)") {
            before {
              subject.upsertDeliveryConfig(deliveryConfig)
            }

            test("preview objects are left alone") {
              expectCatching {
                resourceRepository.get(previewEnvironment.resources.first().id)
              }.isSuccess()

              expectCatching {
                with(previewArtifact) {
                  artifactRepository.get(name, type, reference, configName)
                }
              }.isSuccess()
            }
          }
        }
      }
    }

    context("persisting individual resources") {
      context("resource lifecycle") {
        val resource = SubmittedResource(
          metadata = mapOf("serviceAccount" to "keel@spinnaker"),
          kind = TEST_API_V1.qualify("whatever"),
          spec = DummyResourceSpec(data = "o hai")
        ).normalize(application)

        context("creation") {
          before {
            val updatedConfig = deliveryConfig.copy(
              environments = setOf(firstEnv.copy(resources = setOf(resource)))
            )
            subject.upsertDeliveryConfig(updatedConfig)
          }

          test("stores the resource") {
            val persistedResource = subject.getResource(resource.id)
            expectThat(persistedResource) {
              get(Resource<*>::id) isEqualTo resource.id
              get(Resource<*>::version) isEqualTo 1
              get(Resource<*>::spec)
                .isA<DummyResourceSpec>()
                .get(DummyResourceSpec::data) isEqualTo "o hai"
            }
          }

          test("records that the resource was created") {
            verify {
              resourceRepository.appendHistory(ofType<ResourceCreated>())
            }
          }

          test("will check the resource") {
            expectThat(resourcesDueForCheck())
              .hasSize(1)
              .first()
              .get { id }.isEqualTo(resource.id)
          }

          context("after an update") {
            before {
              resourcesDueForCheck()
              subject.upsertResource(
                resource.copy(
                  spec = DummyResourceSpec(
                    id = resource.spec.id,
                    data = "kthxbye"
                  )
                ), deliveryConfig.name
              )
            }

            test("does not return the updated resource until a new environment version is created") {
              expectThat(subject.getResource(resource.id)) {
                get(Resource<*>::version) isEqualTo 1
                get(Resource<*>::spec)
                  .isA<DummyResourceSpec>()
                  .get(DummyResourceSpec::data) isEqualTo resource.spec.data
              }
            }

            test("records that the resource was updated") {
              verify {
                resourceRepository.appendHistory(ofType<ResourceUpdated>())
              }
            }

            test("will check the resource again") {
              expectThat(resourcesDueForCheck())
                .hasSize(1)
                .first()
                .get { id }.isEqualTo(resource.id)
            }
          }

          context("after a no-op update") {
            before {
              resourcesDueForCheck()
              subject.upsertResource(resource, deliveryConfig.name)
            }

            test("does not update the resource version") {
              expectThat(subject.getResource(resource.id)) {
                get(Resource<*>::version) isEqualTo 1
              }
            }

            test("does not record that the resource was updated") {
              verify(exactly = 0) {
                publisher.publishEvent(ofType<ResourceUpdated>())
              }
            }

            test("will not check the resource again") {
              expectThat(resourcesDueForCheck())
                .isEmpty()
            }
          }
        }
      }
    }

    context("don't allow resources to be managed by more than 1 config") {
      before {
        subject.upsertDeliveryConfig(deliveryConfig)
      }
      test("trying to persist another config with the same resource") {
        expectThrows<DuplicateManagedResourceException> {
          subject.upsertDeliveryConfig(
            secondDeliveryConfig
          )
        }
        expectThrows<NoSuchDeliveryConfigException> { deliveryConfigRepository.get(secondConfigName) }
        expectCatching { resourceRepository.get(firstResource.id) }.isSuccess()
      }
    }

    context("trying to persist two configs with the same name, but different application") {
      before {
        subject.upsertDeliveryConfig(deliveryConfig)
      }
      test("second config fails with exception, first config didn't change") {
        expectThrows<ConflictingDeliveryConfigsException> {
          subject.upsertDeliveryConfig(
            anotherDeliveryConfigWithSameName
          )
        }
        expectThat(deliveryConfigRepository.get(configName))
          .get { application }
          .isEqualTo(deliveryConfig.application)

        expectThrows<NoSuchDeliveryConfigException> {
          deliveryConfigRepository.getByApplication(
            anotherDeliveryConfigWithSameName.application
          )
        }
      }
    }

    context("trying to persist another config with the same application, but different config names") {
      before {
        subject.upsertDeliveryConfig(deliveryConfig)
      }
      test("second config fails with exception, first config didn't change") {
        expectThrows<TooManyDeliveryConfigsException> {
          subject.upsertDeliveryConfig(
            anotherDeliveryConfigWithSameApp
          )
        }
        expectThat(deliveryConfigRepository.get(configName))
          .get { application }
          .isEqualTo(deliveryConfig.application)

        expectThrows<NoSuchDeliveryConfigException> {
          deliveryConfigRepository.get(
            anotherDeliveryConfigWithSameApp.name
          )
        }
      }
    }

    context("storing constraint states") {
      val pendingState = ConstraintState(
        deliveryConfigName = configWithStatefulConstraint.name,
        environmentName = firstEnv.name,
        artifactVersion = "1.1",
        artifactReference = "myartifact",
        type = MANUAL_JUDGEMENT_CONSTRAINT_TYPE,
        status = PENDING,
      )
      before {
        subject.upsertDeliveryConfig(configWithStatefulConstraint)
      }
      context("no previous state") {
        test("event sent when saving config for the first time") {
          subject.storeConstraintState(pendingState)
          verify(exactly = 1) { publisher.publishEvent(ofType<ConstraintStateChanged>()) }
        }
      }
      context("a different previous state") {
        test("event sent when saving config for the first time") {
          subject.storeConstraintState(pendingState)
          val state = ConstraintState(
            deliveryConfigName = configWithStatefulConstraint.name,
            environmentName = firstEnv.name,
            artifactVersion = "1.1",
            artifactReference = artifact.reference,
            type = MANUAL_JUDGEMENT_CONSTRAINT_TYPE,
            status = FAIL,
          )
          subject.storeConstraintState(state)
          verify(exactly = 2) { publisher.publishEvent(ofType<ConstraintStateChanged>()) }
        }
      }
      context("the same previous state") {
        test("event sent only once") {
          subject.storeConstraintState(pendingState)
          subject.storeConstraintState(pendingState)
          verify(exactly = 1) { publisher.publishEvent(ofType<ConstraintStateChanged>()) }
        }
      }
    }

    context("two verifications in pending state") {
      val version = "v1"
      before {
        subject.upsertDeliveryConfig(deliveryConfig)
        val context = ArtifactInEnvironmentContext(deliveryConfig, firstEnv.name, artifact.reference, version)
        subject.updateActionState(context, verification, PENDING)
        subject.updateActionState(context, secondVerification, PENDING)
      }

      context("verification deleted from delivery config") {
        val context = ArtifactInEnvironmentContext(deliveryConfigWithVerificationRemoved, firstEnv.name, artifact.reference, version)

        before {
          subject.upsertDeliveryConfig(deliveryConfigWithVerificationRemoved)
        }

        test("the pending state associated with the deleted verification has changed to override fail") {
          val state = subject.getActionState(context, verification)
          expectThat(state)
            .isNotNull()
            .get { status }
            .isEqualTo(OVERRIDE_FAIL)
        }

        test("the pending state associated with the remaining verification is still present") {
          val state = subject.getActionState(context, secondVerification)
          expectThat(state)
            .isNotNull()
            .get { status }
            .isEqualTo(PENDING)
        }
      }
    }

    context("re-registering an artifact does not publish an event") {
      before {
        subject.upsertDeliveryConfig(deliveryConfig)
      }

      test("first upsert triggers an event") {
        verify(exactly = 1) {
          publisher.publishEvent(ofType<ArtifactRegisteredEvent>())
        }
      }

      test("second upsert does not trigger an event") {
        subject.upsertDeliveryConfig(deliveryConfig)
        verify(exactly = 1) { // still at 1
          publisher.publishEvent(ofType<ArtifactRegisteredEvent>())
        }
      }
    }
  }
}
