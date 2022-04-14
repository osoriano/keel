package com.netflix.spinnaker.keel.preview

import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Tag
import com.netflix.spectator.api.Timer
import com.netflix.spinnaker.keel.api.ArtifactReferenceProvider
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.DependencyType.LOAD_BALANCER
import com.netflix.spinnaker.keel.api.DependencyType.SECURITY_GROUP
import com.netflix.spinnaker.keel.api.Dependent
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.InvalidMonikerException
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.NamedResource
import com.netflix.spinnaker.keel.api.PreviewEnvironmentSpec
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.artifacts.ArtifactOriginFilter
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.branchName
import com.netflix.spinnaker.keel.api.artifacts.branchStartsWith
import com.netflix.spinnaker.keel.api.ec2.ApplicationLoadBalancerSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterDependencies
import com.netflix.spinnaker.keel.api.ec2.EC2_CLUSTER_V1
import com.netflix.spinnaker.keel.api.ec2.EC2_SECURITY_GROUP_V1
import com.netflix.spinnaker.keel.api.ec2.SecurityGroupSpec
import com.netflix.spinnaker.keel.api.ec2.old.ClusterV1Spec
import com.netflix.spinnaker.keel.api.ec2.old.ClusterV1Spec.ImageProvider
import com.netflix.spinnaker.keel.application.ApplicationConfig
import com.netflix.spinnaker.keel.core.api.ManualJudgementConstraint
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.core.api.SubmittedEnvironment
import com.netflix.spinnaker.keel.core.api.TagAmiPostDeployAction
import com.netflix.spinnaker.keel.core.name
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.api.Application
import com.netflix.spinnaker.keel.api.DataSources
import com.netflix.spinnaker.keel.graphql.resources.GraphqlSchemaHandler.Companion.GRAPHQL_SCHEMA_V1
import com.netflix.spinnaker.keel.graphql.resources.GraphqlSchemaSpec
import com.netflix.spinnaker.keel.igor.DeliveryConfigImporter
import com.netflix.spinnaker.keel.notifications.DeliveryConfigImportFailed
import com.netflix.spinnaker.keel.notifications.DismissibleNotification
import com.netflix.spinnaker.keel.persistence.ApplicationRepository
import com.netflix.spinnaker.keel.persistence.DismissibleNotificationRepository
import com.netflix.spinnaker.keel.persistence.EnvironmentDeletionRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.preview.PreviewEnvironmentCodeEventListener.Companion.APPLICATION_RETRIEVAL_ERROR
import com.netflix.spinnaker.keel.preview.PreviewEnvironmentCodeEventListener.Companion.CODE_EVENT_COUNTER
import com.netflix.spinnaker.keel.preview.PreviewEnvironmentCodeEventListener.Companion.COMMIT_HANDLING_DURATION
import com.netflix.spinnaker.keel.preview.PreviewEnvironmentCodeEventListener.Companion.DELIVERY_CONFIG_NOT_FOUND
import com.netflix.spinnaker.keel.preview.PreviewEnvironmentCodeEventListener.Companion.PREVIEW_ENVIRONMENT_MARK_FOR_DELETION_SUCCESS
import com.netflix.spinnaker.keel.preview.PreviewEnvironmentCodeEventListener.Companion.PREVIEW_ENVIRONMENT_UPSERT_ERROR
import com.netflix.spinnaker.keel.preview.PreviewEnvironmentCodeEventListener.Companion.PREVIEW_ENVIRONMENT_UPSERT_SUCCESS
import com.netflix.spinnaker.keel.retrofit.RETROFIT_NOT_FOUND
import com.netflix.spinnaker.keel.retrofit.RETROFIT_UNAUTHORIZED
import com.netflix.spinnaker.keel.scm.DELIVERY_CONFIG_RETRIEVAL_ERROR
import com.netflix.spinnaker.keel.scm.DELIVERY_CONFIG_RETRIEVAL_SUCCESS
import com.netflix.spinnaker.keel.scm.PrDeclinedEvent
import com.netflix.spinnaker.keel.scm.PrDeletedEvent
import com.netflix.spinnaker.keel.scm.PrMergedEvent
import com.netflix.spinnaker.keel.scm.PrOpenedEvent
import com.netflix.spinnaker.keel.scm.PrUpdatedEvent
import com.netflix.spinnaker.keel.scm.ScmUtils
import com.netflix.spinnaker.keel.scm.toTags
import com.netflix.spinnaker.keel.test.applicationLoadBalancer
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import com.netflix.spinnaker.keel.test.debianArtifact
import com.netflix.spinnaker.keel.test.dockerArtifact
import com.netflix.spinnaker.keel.test.locatableResource
import com.netflix.spinnaker.keel.test.resource
import com.netflix.spinnaker.keel.test.submittedResource
import com.netflix.spinnaker.keel.test.titusCluster
import com.netflix.spinnaker.keel.validators.DeliveryConfigValidator
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
import io.mockk.spyk
import org.springframework.context.ApplicationEventPublisher
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.contains
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.containsKeys
import strikt.assertions.endsWith
import strikt.assertions.first
import strikt.assertions.hasSize
import strikt.assertions.isA
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isLessThanOrEqualTo
import strikt.assertions.isTrue
import strikt.assertions.length
import strikt.assertions.none
import strikt.assertions.one
import java.time.Clock
import java.time.Duration
import kotlin.reflect.KClass
import io.mockk.coEvery as every
import io.mockk.coVerify as verify

internal class PreviewEnvironmentCodeEventListenerTests : JUnit5Minutests {
  class Fixture {
    private val objectMapper = configuredTestObjectMapper()
    private val clock: Clock = MutableClock()
    val fakeTimer: Timer = mockk()
    val repository: KeelRepository = mockk()
    val environmentDeletionRepository: EnvironmentDeletionRepository = mockk()
    val notificationRepository: DismissibleNotificationRepository = mockk()
    val importer: DeliveryConfigImporter = mockk()
    val front50Cache: Front50Cache = mockk()
    val springEnv: org.springframework.core.env.Environment = mockk()
    val spectator: Registry = mockk()
    val eventPublisher: ApplicationEventPublisher = mockk()
    val validator: DeliveryConfigValidator = mockk()
    val scmUtils: ScmUtils = mockk()
    val applicationRepository: ApplicationRepository = mockk()

    val subject = spyk(
      PreviewEnvironmentCodeEventListener(
        repository = repository,
        environmentDeletionRepository = environmentDeletionRepository,
        notificationRepository = notificationRepository,
        deliveryConfigImporter = importer,
        deliveryConfigValidator = validator,
        front50Cache = front50Cache,
        objectMapper = objectMapper,
        springEnv = springEnv,
        spectator = spectator,
        clock = clock,
        eventPublisher = eventPublisher,
        scmUtils = scmUtils,
        applicationRepository = applicationRepository
      )
    )

    val front50App = Application(
      name = "fnord",
      email = "keel@keel.io",
      repoType = "stash",
      repoProjectKey = "myorg",
      repoSlug = "myrepo",
      dataSources = DataSources(enabled = emptyList(), disabled = emptyList())
    )

    val appConfig = ApplicationConfig(application = "fnord", autoImport = true)

    val dockerFromMain = dockerArtifact()

    val dockerFromBranch = dockerFromMain.copy(
      reference = "docker-from-branch",
      from = ArtifactOriginFilter(branch = branchStartsWith("feature/"))
    )

    val dockerWithNonMatchingFilter = dockerFromBranch.copy(
      reference = "fnord-non-matching",
      from = ArtifactOriginFilter(branch = branchName("not-the-right-branch"))
    )

    val applicationLoadBalancer = applicationLoadBalancer()

    val cluster = titusCluster(artifact = dockerFromMain)

    val defaultAppSecurityGroup = Resource(
      kind = EC2_SECURITY_GROUP_V1.kind,
      metadata = mapOf("id" to "fnord", "application" to "fnord", "displayName" to "fnord"),
      spec = SecurityGroupSpec(
        moniker = Moniker("fnord"),
        locations = cluster.spec.locations,
        description = "default app security group"
      )
    )

    val defaultElbSecurityGroup = Resource(
      kind = EC2_SECURITY_GROUP_V1.kind,
      metadata = mapOf("id" to "fnord", "application" to "fnord", "displayName" to "fnord-elb"),
      spec = SecurityGroupSpec(
        moniker = Moniker("fnord", "elb"),
        locations = cluster.spec.locations,
        description = "default load balancer security group"
      )
    )

    val clusterNamedAfterApp = titusCluster(
      moniker = Moniker("fnord"),
      artifact = dockerFromMain
    )

    val clusterWithDependencies = titusCluster(
      moniker = Moniker("fnord", "dependent"),
      artifact = dockerFromMain
    ).run {
      copy(
        spec = spec.copy(
          _defaults = spec.defaults.copy(
            dependencies = ClusterDependencies(
              loadBalancerNames = setOf(applicationLoadBalancer.name),
              securityGroupNames = setOf(defaultAppSecurityGroup.name, defaultElbSecurityGroup.name)
            )
          )
        )
      )
    }

    val debianFromMain = debianArtifact()
    val debianFromBranch = debianFromMain.copy(
      reference = "debian-from-branch",
      from = ArtifactOriginFilter(branchStartsWith("feature/"))
    )

    @Suppress("DEPRECATION")
    val clusterWithOldSpecVersion = resource(
      kind = EC2_CLUSTER_V1.kind,
      spec = ClusterV1Spec(
        moniker = Moniker("fnord", "old"),
        imageProvider = ImageProvider(debianFromMain.reference),
        locations = applicationLoadBalancer.spec.locations
      )
    )

    /**
     * This is a resource where isPreviewable() == false
     */
    val graphqlSchema = resource(
      kind = GRAPHQL_SCHEMA_V1.kind,
      spec = GraphqlSchemaSpec(
        application = "fnord",
        artifactReference = "fnord-docker",
        edge = "enterprise",
        registryEnv = "test"
      )
    )

    val resourceIgnoredByName = resource()
    val resourceIgnoredByKind = locatableResource()

    var deliveryConfig = DeliveryConfig(
      application = "fnord",
      name = "myconfig",
      serviceAccount = "keel@keel.io",
      artifacts = setOf(dockerFromMain, debianFromMain),
      environments = setOf(
        Environment(
          name = "test",
          resources = setOf(
            applicationLoadBalancer,
            cluster,
            clusterNamedAfterApp, // name conflict with default sec group, but different kind
            defaultAppSecurityGroup,
            defaultElbSecurityGroup,
            clusterWithDependencies,
            clusterWithOldSpecVersion,
            graphqlSchema,
            resourceIgnoredByName,
            resourceIgnoredByKind
          ),
          constraints = setOf(ManualJudgementConstraint()),
          postDeploy = listOf(TagAmiPostDeployAction())
        )
      ),
      previewEnvironments = setOf(
        PreviewEnvironmentSpec(
          branch = branchStartsWith("feature/"),
          baseEnvironment = "test",
          excludeResources = setOf(
            NamedResource(resourceIgnoredByName.kind, resourceIgnoredByName.name),
            NamedResource(resourceIgnoredByKind.kind, "*")
          )
        )
      )
    )

    val previewEnvSpec = deliveryConfig.previewEnvironments.first()
    val previewEnvSlot = slot<Environment>()
    val previewArtifactsSlot = slot<Set<DeliveryArtifact>>()
    val previewEnv by lazy { previewEnvSlot.captured }
    val previewArtifacts by lazy { previewArtifactsSlot.captured }

    val submittedDeliveryConfig = with(deliveryConfig) {
      SubmittedDeliveryConfig(
        application = application,
        name = name,
        serviceAccount = serviceAccount,
        metadata = metadata,
        artifacts = artifacts,
        previewEnvironments = previewEnvironments,
        environments = environments.map { env ->
          SubmittedEnvironment(
            name = env.name,
            resources = env.resources.map { res ->
              submittedResource(res.kind, res.spec)
            }.toSet(),
            constraints = env.constraints,
            postDeploy = env.postDeploy
          )
        }.toSet()
      )
    }

    fun setupMocks() {
      every {
        springEnv.getProperty("keel.previewEnvironments.enabled", Boolean::class.java, true)
      } returns true

      every {
        spectator.counter(any(), any<Iterable<Tag>>())
      } returns mockk {
        every {
          increment()
        } just runs
      }

      every {
        validator.validate(any())
      } just runs

      every {
        spectator.timer(any(), any<Iterable<Tag>>())
      } returns fakeTimer

      every {
        fakeTimer.record(any<Duration>())
      } just runs

      every {
        eventPublisher.publishEvent(any<Object>())
      } just runs

      every {
        repository.allDeliveryConfigs(any())
      } returns setOf(deliveryConfig)

      every {
        front50Cache.applicationByName(deliveryConfig.application)
      } returns front50App

      every {
        applicationRepository.get(deliveryConfig.application)
      } returns appConfig

      every {
        importer.import(any(), manifestPath = any())
      } returns submittedDeliveryConfig

      every { repository.getDeliveryConfig(deliveryConfig.name) } returns deliveryConfig

      every {
        repository.upsertPreviewEnvironment(any(), capture(previewEnvSlot), capture(previewArtifactsSlot))
      } just runs

      every { environmentDeletionRepository.markForDeletion(any()) } just runs

      every {
        notificationRepository.dismissNotification(any<KClass<DismissibleNotification>>(), any(), any())
      } returns true

      every {
        scmUtils.getPullRequestLink(any())
      } returns "https://commit-link.org"
    }
  }

  val prOpenedEvent = PrOpenedEvent(
    repoKey = "stash/myorg/myrepo",
    targetProjectKey = "myorg",
    targetRepoSlug = "myrepo",
    pullRequestBranch = "feature/abc",
    targetBranch = "main",
    pullRequestId = "42"
  )

  val prUpdatedEvent = PrUpdatedEvent(
    repoKey = "stash/myorg/myrepo",
    targetProjectKey = "myorg",
    targetRepoSlug = "myrepo",
    pullRequestBranch = "feature/abc",
    targetBranch = "main",
    pullRequestId = "42"
  )

  val prMergedEvent = PrMergedEvent(
    repoKey = "stash/myorg/myrepo",
    targetProjectKey = "myorg",
    targetRepoSlug = "myrepo",
    pullRequestBranch = "feature/abc",
    targetBranch = "main",
    pullRequestId = "42",
    commitHash = "a34afb13b"
  )

  val prDeclinedEvent = PrDeclinedEvent(
    repoKey = "stash/myorg/myrepo",
    targetProjectKey = "myorg",
    targetRepoSlug = "myrepo",
    pullRequestBranch = "feature/abc",
    targetBranch = "main",
    pullRequestId = "42"
  )

  val prDeletedEvent = PrDeletedEvent(
    repoKey = "stash/myorg/myrepo",
    targetProjectKey = "myorg",
    targetRepoSlug = "myrepo",
    pullRequestBranch = "feature/abc",
    targetBranch = "main",
    pullRequestId = "42"
  )

  fun tests() = rootContext<Fixture> {
    fixture { Fixture() }

    context("a delivery config exists in a branch") {
      before {
        setupMocks()
      }

      listOf(prOpenedEvent, prUpdatedEvent).forEach { prEvent ->
        context("a PR event matching a preview environment spec is received") {
          before {
            subject.handlePrEvent(prEvent)
          }

          test("the delivery config is imported from the branch in the PR event") {
            verify(exactly = 1) {
              importer.import(
                codeEvent = any(),
                manifestPath = any(),
              )
            }
          }

          test("delivery config import failure notification is dismissed on successful import") {
            verify {
              notificationRepository.dismissNotification(
                any<KClass<DismissibleNotification>>(),
                deliveryConfig.application,
                prEvent.pullRequestBranch,
                any()
              )
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

          test("a successful preview environment upsert is counted") {
            val tags = mutableListOf<Iterable<Tag>>()
            verify {
              spectator.counter(CODE_EVENT_COUNTER, capture(tags))
            }
            expectThat(tags).one {
              contains(PREVIEW_ENVIRONMENT_UPSERT_SUCCESS.toTags())
            }
          }

          test("a duration is recorded for successful handling of the PR event") {
            verify(exactly = 1) {
              spectator.timer(COMMIT_HANDLING_DURATION, any<Iterable<Tag>>())
              fakeTimer.record(any<Duration>())
            }
          }

          test("the preview environment is stored in the database") {
            verify {
              repository.upsertPreviewEnvironment(any(), any(), any())
            }
            expectThat(previewEnv.isPreview).isTrue()
          }

          test("preview artifacts are stored in the database") {
            expectThat(previewArtifacts) {
              one {
                get { name }.isEqualTo(dockerFromMain.name)
                get { type }.isEqualTo(dockerFromMain.type)
                get { from!!.branch }.isEqualTo(previewEnvSpec.branch)
              }
              one {
                get { name }.isEqualTo(debianFromMain.name)
                get { type }.isEqualTo(debianFromMain.type)
                get { from!!.branch }.isEqualTo(previewEnvSpec.branch)
              }
            }
          }

          test("the preview environment has no constraints or post-deploy actions") {
            expectThat(previewEnv.constraints).isEmpty()
            expectThat(previewEnv.postDeploy).isEmpty()
          }

          test("the name of the preview environment is generated correctly") {
            val baseEnv = deliveryConfig.environments.first()
            val suffix = prEvent.pullRequestBranch.shortHash

            expectThat(previewEnv) {
              get { name }.isEqualTo("${baseEnv.name}-$suffix")
            }
          }

          test("relevant metadata is added to the preview environment") {
            expectThat(previewEnv.metadata).containsKeys("basedOn", "repoKey", "branch", "pullRequestId")
          }

          test("resource names/IDs are updated with branch hash") {
            val baseEnv = deliveryConfig.environments.first()
            val baseResource = baseEnv.resources.first()
            val previewResource = previewEnv.resources.first()

            expectThat(previewResource).run {
              isEqualTo(baseResource.deepRename(prEvent.pullRequestBranch.shortHash))
              get { name }.endsWith(prEvent.pullRequestBranch.shortHash)
              get { id }.endsWith(prEvent.pullRequestBranch.shortHash)
            }
          }

          test("the artifact reference in a resource is updated to match the preview artifact") {
            expectThat(previewEnv.resources.find { it.basedOn == cluster.id }?.spec)
              .isA<ArtifactReferenceProvider>()
              .get { artifactReference }.isEqualTo(previewArtifacts.find { it.type == DOCKER }!!.reference)

            expectThat(previewEnv.resources.find { it.basedOn == clusterWithOldSpecVersion.id }?.spec)
              // this also demonstrates that the old cluster spec supports the standard artifact reference interface
              .isA<ArtifactReferenceProvider>()
              .get { artifactReference }.isEqualTo(previewArtifacts.find { it.type == DEBIAN }!!.reference)
          }

          test("the names of resource dependencies present in the preview environment are adjusted to match") {
            val suffix = prEvent.pullRequestBranch.shortHash
            val dependency = applicationLoadBalancer

            expectThat(previewEnv.resources.find { it.basedOn == clusterWithDependencies.id }?.spec)
              .isA<Dependent>()
              .get { dependsOn.first { it.type == LOAD_BALANCER }.name }
              .isEqualTo(dependency.spec.moniker.withSuffix(suffix, maxNameLength = ApplicationLoadBalancerSpec.MAX_NAME_LENGTH).name)
          }

          test("the names of the default security groups are not changed in the dependencies") {
            expectThat(previewEnv.resources.find { it.basedOn == clusterWithDependencies.id }?.spec)
              .isA<Dependent>()
              .get { dependsOn.filter { it.type == SECURITY_GROUP }.map { it.name }.toSet() }
              .containsExactlyInAnyOrder(defaultAppSecurityGroup.name, defaultElbSecurityGroup.name)
          }

          test("non-previewable resources do not appear in the preview environment") {
            val nonPreviewableResources = previewEnv.resources.filterNot { it.spec.isPreviewable() }
            expectThat(nonPreviewableResources).isEmpty()
          }

          test("specific resources can be ignored by name") {
            expectThat(previewEnv.resources).none { get { kind }.isEqualTo(resourceIgnoredByName.kind) }
          }

          test("resources can be ignored by kind") {
            expectThat(previewEnv.resources).none { get { kind }.isEqualTo(resourceIgnoredByKind.kind) }
          }
        }

        context("include only specific resources") {
          modifyFixture {
            deliveryConfig = deliveryConfig.copy(
              previewEnvironments = setOf(
                previewEnvSpec.copy(
                  excludeResources = emptySet(),
                  includeResources = setOf(
                    NamedResource(cluster.kind, cluster.name)
                  )
                )
              )
            )

            every { importer.import(any(), manifestPath = any()) } returns submittedDeliveryConfig.copy(
              previewEnvironments = deliveryConfig.previewEnvironments
            )
            every { repository.getDeliveryConfig(deliveryConfig.name) } returns deliveryConfig

            every { repository.allDeliveryConfigs(any()) } returns setOf(deliveryConfig)
          }

          before {
            subject.handlePrEvent(prEvent)
          }

          test("only one resource should be in the preview environment") {
            expectThat(previewEnv.resources).hasSize(1).first().get { kind }.isEqualTo(cluster.kind)
          }
        }
      }

      context("an app with custom manifest path") {
        val deliveryConfigPath = "custom/spinnaker.yml"

        before {
          every {
            applicationRepository.get(deliveryConfig.application)
          } returns appConfig.copy(deliveryConfigPath = deliveryConfigPath)

        }

        test("importing the manifest from the correct path") {
          subject.handlePrEvent(prOpenedEvent)
          verify(exactly = 1) {
            importer.import(
              codeEvent = any(),
              manifestPath = deliveryConfigPath
            )
          }
        }
      }

      context("a PR event NOT matching a preview environment spec is received") {
        before {
          val nonMatchingPrEvent = prOpenedEvent.copy(pullRequestBranch = "not-a-match")
          subject.handlePrEvent(nonMatchingPrEvent)
        }

        testEventIgnored()

        test("a delivery config not found is counted") {
          val tags = mutableListOf<Iterable<Tag>>()
          verify {
            spectator.counter(CODE_EVENT_COUNTER, capture(tags))
          }
          expectThat(tags).one {
            contains(DELIVERY_CONFIG_NOT_FOUND.toTags())
          }
        }

        test("duration metric is not recorded") {
          verify {
            fakeTimer wasNot called
          }
        }
      }

      context("a PR event not associated with a PR is received") {
        before {
          subject.handlePrEvent(prOpenedEvent.copy(pullRequestId = "-1"))
        }

        testEventIgnored()
      }

      listOf(prMergedEvent, prDeclinedEvent, prDeletedEvent).forEach { prEvent ->
        context("a ${prEvent::class.simpleName} event matching a preview environment spec is received") {
          before {
            // just to trigger saving the preview environment
            subject.handlePrEvent(prEvent)

            every {
              repository.getDeliveryConfig(deliveryConfig.name)
            } returns deliveryConfig.copy(
              environments = deliveryConfig.environments + setOf(previewEnv)
            )

            subject.handlePrFinished(prEvent)
          }

          test("the matching preview environment is marked for deletion") {
            verify {
              environmentDeletionRepository.markForDeletion(previewEnv)
            }
          }

          test("a metric is counted for successfully marking for deletion") {
            val tags = mutableListOf<Iterable<Tag>>()
            verify {
              spectator.counter(CODE_EVENT_COUNTER, capture(tags))
            }
            expectThat(tags).one {
              contains(PREVIEW_ENVIRONMENT_MARK_FOR_DELETION_SUCCESS.toTags())
            }
          }
        }
      }
    }

    context("a PR event matching a preview spec branch filter but from a different app's repo") {
      before {
        setupMocks()

        every {
          front50Cache.applicationByName("fnord")
        } returns front50App.copy(
          repoProjectKey = "anotherorg",
          repoSlug = "another-repo"
        )

        subject.handlePrEvent(prOpenedEvent)
      }

      test("event is ignored") {
        verify(exactly = 0) {
          repository.upsertDeliveryConfig(any<DeliveryConfig>())
        }
        verify {
          importer wasNot called
        }
      }
    }

    context("with feature flag disabled") {
      modifyFixture {
        every {
          springEnv.getProperty("keel.previewEnvironments.enabled", Boolean::class.java, true)
        } returns false
      }

      before {
        val nonMatchingPrEvent = prOpenedEvent.copy(pullRequestBranch = "not-a-match")
        subject.handlePrEvent(nonMatchingPrEvent)
      }

      testEventIgnored()
    }

    context("other error scenarios") {
      before {
        setupMocks()
      }

      context("failure to retrieve delivery config from branch") {
        context("when file is not found") {
          modifyFixture {
            every {
              importer.import(any(), manifestPath = any())
            } throws RETROFIT_NOT_FOUND
          }

          before {
            subject.handlePrEvent(prOpenedEvent)
          }

          test("we fall back to the delivery config in the database") {
            verify {
              repository.getDeliveryConfig(deliveryConfig.name)
            }
          }

          test("the preview environment is stored in the database") {
            verify {
              repository.upsertPreviewEnvironment(any(), any(), any())
            }
            expectThat(previewEnv.isPreview).isTrue()
          }
        }

        context("when keel doesn't have permission to access the repo") {
          modifyFixture {
            every {
              importer.import(any(), manifestPath = any())
            } throws RETROFIT_UNAUTHORIZED
          }

          before {
            subject.handlePrEvent(prOpenedEvent)
          }

          test("we fall back to the delivery config in the database") {
            verify {
              repository.getDeliveryConfig(deliveryConfig.name)
            }
          }

          test("the preview environment is stored in the database") {
            verify {
              repository.upsertPreviewEnvironment(any(), any(), any())
            }
            expectThat(previewEnv.isPreview).isTrue()
          }

        }

        context("with other exceptions") {
          modifyFixture {
            every {
              importer.import(any(), manifestPath = any())
            } throws SystemException("oh noes!")
          }

          before {
            subject.handlePrEvent(prOpenedEvent)
          }

          test("a delivery config retrieval error is counted") {
            val tags = mutableListOf<Iterable<Tag>>()
            verify {
              spectator.counter(CODE_EVENT_COUNTER, capture(tags))
            }
            expectThat(tags).one {
              contains(DELIVERY_CONFIG_RETRIEVAL_ERROR.toTags())
            }
          }

          test("an event is published") {
            val failureEvent = slot<DeliveryConfigImportFailed>()
            verify {
              eventPublisher.publishEvent(capture(failureEvent))
            }
            expectThat(failureEvent.captured.branch).isEqualTo(prOpenedEvent.pullRequestBranch)
          }
        }
      }

      context("failure to retrieve application") {
        modifyFixture {
          every {
            front50Cache.applicationByName(deliveryConfig.application)
          } throws SystemException("oh noes!")
        }

        before {
          subject.handlePrEvent(prOpenedEvent)
        }

        test("an application retrieval error is counted") {
          val tags = mutableListOf<Iterable<Tag>>()
          verify {
            spectator.counter(CODE_EVENT_COUNTER, capture(tags))
          }
          expectThat(tags).one {
            contains(APPLICATION_RETRIEVAL_ERROR.toTags())
          }
        }
      }

      context("failure to update database") {
        modifyFixture {
          every {
            repository.upsertPreviewEnvironment(any(), any(), any())
          } throws SystemException("oh noes!")
        }

        before {
          subject.handlePrEvent(prOpenedEvent)
        }

        test("an upsert error is counted") {
          val tags = mutableListOf<Iterable<Tag>>()
          verify {
            spectator.counter(CODE_EVENT_COUNTER, capture(tags))
          }
          expectThat(tags).one {
            contains(PREVIEW_ENVIRONMENT_UPSERT_ERROR.toTags())
          }
        }
      }
    }

    context("renaming mechanism") {
      val baseMaxLength = 32
      test("trim apps with a long name") {
        val moniker = Moniker(app = "spkr12345678910", stack = "test", detail = "test")
        expectThat(moniker.withSuffix("4f902bf", maxNameLength = baseMaxLength).toName()).and {
          isEqualTo("spkr12345678910-test-tes-4f902bf")
          length.isLessThanOrEqualTo(baseMaxLength)
        }
      }
      test("apps with no stack") {
        val moniker = Moniker(app = "spkr12345678910", detail = "test")
        expectThat(moniker.withSuffix("4f902bf", maxNameLength = baseMaxLength).toName()).and {
          isEqualTo("spkr12345678910--test-4f902bf")
          length.isLessThanOrEqualTo(baseMaxLength)
        }
      }

      test("apps with no detail") {
        val moniker = Moniker(app = "spkr12345678910", stack = "test")
        expectThat(moniker.withSuffix("4f902bf", maxNameLength = baseMaxLength).toName()).and {
          isEqualTo("spkr12345678910-test-4f902bf")
          length.isLessThanOrEqualTo(baseMaxLength)
        }
      }

      test("apps with no stack and detail") {
        val moniker = Moniker(app = "spkr12345678910")
        expectThat(moniker.withSuffix("4f902bf", maxNameLength = baseMaxLength).toName()).and {
          isEqualTo("spkr12345678910--4f902bf")
          length.isLessThanOrEqualTo(baseMaxLength)
        }
      }
      test("larger max length") {
        val moniker = Moniker(app = "spkr12345678910", stack = "test", detail = "test")
        expectThat(moniker.withSuffix("4f902bf", maxNameLength = 100).toName()).and {
          isEqualTo("spkr12345678910-test-test-4f902bf")
          length.isLessThanOrEqualTo(100)
        }
      }

      test("smaller max length with truncate") {
        val moniker = Moniker(app = "spkr12345678910", stack = "test", detail = "test")
        expectThat(moniker.withSuffix("4f902bf", canTruncateStack = true, maxNameLength = 26).toName()).and {
          isEqualTo("spkr12345678910-4f902bf")
          length.isLessThanOrEqualTo(26)
        }
      }

      test("using short suffix") {
        val moniker = Moniker(app = "spkr012345678901234567890123", stack = "test", detail = "test")
        expectThat(moniker.withSuffix("4f902bf", canTruncateStack = true, maxNameLength = baseMaxLength).toName()).and {
          isEqualTo("spkr012345678901234567890123-4f9")
        }
      }

      test("app name is too long") {
        val moniker = Moniker(app = "spkr12345678910123412345678910123456", stack = "test", detail = "test")
        expectThrows<InvalidMonikerException> {
          moniker.withSuffix("4f902bf", maxNameLength = baseMaxLength).toName()
        }
      }

      test("app name is too long even without stack") {
        val moniker = Moniker(app = "spkr0123456789012345678901234567890123456", stack = "test", detail = "test")
        expectThrows<InvalidMonikerException> {
          moniker.withSuffix("4f902bf", canTruncateStack = true, maxNameLength = baseMaxLength).toName()
        }
      }
    }
  }

  private fun TestContextBuilder<Fixture, Fixture>.testEventIgnored() {
    test("event is ignored") {
      verify(exactly = 0) {
        repository.upsertDeliveryConfig(any<DeliveryConfig>())
      }
      verify {
        importer wasNot called
      }
    }
  }
}
