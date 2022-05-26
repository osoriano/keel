package com.netflix.spinnaker.keel.dgs

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.graphql.dgs.DgsQueryExecutor
import com.netflix.graphql.dgs.autoconfig.DgsAutoConfiguration
import com.netflix.spinnaker.keel.actuation.ExecutionSummaryService
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.SubnetAwareRegionSpec
import com.netflix.spinnaker.keel.api.action.ActionRepository
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.api.artifacts.CurrentlyDeployedVersion
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.Repo
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.ec2.EC2_CLUSTER_V1_1
import com.netflix.spinnaker.keel.api.migration.ApplicationMigrationStatus
import com.netflix.spinnaker.keel.api.migration.UserGeneratedConfig
import com.netflix.spinnaker.keel.artifacts.ArtifactVersionLinks
import com.netflix.spinnaker.keel.auth.AuthorizationSupport
import com.netflix.spinnaker.keel.buoy.BuoyClient
import com.netflix.spinnaker.keel.core.api.ActuationPlan
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactPin
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVeto
import com.netflix.spinnaker.keel.core.api.PinType
import com.netflix.spinnaker.keel.core.api.PromotionStatus
import com.netflix.spinnaker.keel.core.api.PublishedArtifactInEnvironment
import com.netflix.spinnaker.keel.core.api.normalize
import com.netflix.spinnaker.keel.exceptions.InvalidAppNameException
import com.netflix.spinnaker.keel.export.ExportService
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.Front50Service
import com.netflix.spinnaker.keel.front50.model.ServiceAccount
import com.netflix.spinnaker.keel.graphql.types.MD_ActuationPlanStatus
import com.netflix.spinnaker.keel.graphql.types.MD_Migration
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationStatus
import com.netflix.spinnaker.keel.graphql.types.MD_UserPermissions
import com.netflix.spinnaker.keel.graphql.types.MD_ValidateResult
import com.netflix.spinnaker.keel.igor.DeliveryConfigImporter
import com.netflix.spinnaker.keel.migrations.ApplicationPrData
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.persistence.ApplicationRepository
import com.netflix.spinnaker.keel.persistence.ArtifactRepository
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.persistence.DiffFingerprintRepository
import com.netflix.spinnaker.keel.persistence.DismissibleNotificationRepository
import com.netflix.spinnaker.keel.persistence.EnvironmentDeletionRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.LifecycleEventRepository
import com.netflix.spinnaker.keel.persistence.TaskTrackingRepository
import com.netflix.spinnaker.keel.scm.ScmUtils
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.services.ApplicationService
import com.netflix.spinnaker.keel.services.ResourceStatusService
import com.netflix.spinnaker.keel.test.debianArtifact
import com.netflix.spinnaker.keel.test.deliveryConfig
import com.netflix.spinnaker.keel.test.submittedDeliveryConfig
import com.netflix.spinnaker.keel.test.submittedResource
import com.netflix.spinnaker.keel.upsert.DeliveryConfigUpserter
import com.netflix.spinnaker.keel.veto.unhappy.UnhappyVeto
import com.netflix.spinnaker.test.dgs.DgsTestConfig
import com.netflix.spinnaker.time.MutableClock
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Runs
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.just
import io.mockk.runs
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import strikt.assertions.isSuccess
import strikt.assertions.isTrue
import java.time.Instant

@SpringBootTest(
  classes = [DgsAutoConfiguration::class, DgsTestConfig::class],
)
class QueryTests {

  @Autowired
  lateinit var dgsQueryExecutor: DgsQueryExecutor
  
  @Autowired
  lateinit var authorizationSupport: AuthorizationSupport

  @Autowired
  lateinit var keelRepository: KeelRepository

  @Autowired
  lateinit var artifactRepository: ArtifactRepository

  @Autowired
  lateinit var front50Service: Front50Service

  @Autowired
  lateinit var applicationService: ApplicationService

  @Autowired
  lateinit var deliveryConfigRepository: DeliveryConfigRepository

  val mapper = configuredObjectMapper()

  val clock = MutableClock()

  private val artifact = debianArtifact()

  private val resource = submittedResource(
    kind = EC2_CLUSTER_V1_1.kind,
    spec = ClusterSpec(
      moniker = Moniker("fnord"),
      artifactReference = artifact.reference,
      locations = SubnetAwareLocations(
        account = "test",
        vpc = "vpc0",
        subnet = "internal (vpc0)",
        regions = setOf(
          SubnetAwareRegionSpec(
            name = "us-east-1",
            availabilityZones = setOf()
          )
        )
      )
    )
  )

  private val repoSlug = "keel"
  private val projectKey = "spkr"
  private val user = "user"

  private val artifactVersion = PublishedArtifact(
    name = artifact.name,
    reference = artifact.reference,
    type = artifact.type,
    version = "v123",
    gitMetadata = GitMetadata(
      commit = "abc123",
      author = "emburns",
      project = projectKey,
      branch = "main",
      repo = Repo(name = repoSlug),
      commitInfo = Commit(
        sha = "abc123",
        message = "I committed this"
      )
    ),
    buildMetadata = BuildMetadata(
      id = 2,
      number = "2"
    )
  )

  private val submittedDeliveryConfig = submittedDeliveryConfig(artifact = artifact, resource = resource)
  private val deliveryConfig = deliveryConfig(artifact = artifact, resources = setOf(resource.normalize("fnord")))

  @BeforeEach
  fun setup() {
    every {
      keelRepository.getDeliveryConfigForApplication(any())
    } returns deliveryConfig

    every {
      deliveryConfigRepository.getByApplication(any())
    } returns deliveryConfig

    every {
      keelRepository.getAllVersionsForEnvironment(artifact, deliveryConfig, "test")
    } returns listOf(
      PublishedArtifactInEnvironment(
        artifact.toArtifactVersion(version = "v1", metadata = mapOf("branch" to "main", "commitId" to "f80cfcfdec37df59604b2ef93dfb29bade340791", "buildNumber" to "41")),
        status = PromotionStatus.CURRENT,
        environmentName = "test"
      )
    )

    every {
      keelRepository.getLatestApprovedInEnvArtifactVersion(any(),any(), any(), any())
    } returns artifactVersion

    every {
      applicationService.pin(any(), any(), any())
    } just Runs

    every {
      applicationService.markAsVetoedIn(any(), any(), any(), any())
    } just Runs

    every {
      applicationService.parseAndValidateDeliveryConfig(any())
    } returns submittedDeliveryConfig

    every {
      authorizationSupport.hasApplicationPermission("WRITE", "APPLICATION", any())
    } returns true

    every {
      authorizationSupport.hasServiceAccountAccess(any())
    } returns true

    coEvery {
      applicationService.openMigrationPr(any(), any(), any())
    } returns Pair(
      ApplicationPrData(
        autoGeneratedConfig = submittedDeliveryConfig,
        repoSlug = repoSlug,
        projectKey = projectKey
      ), "http://link-to-pr"
    )

    coEvery {
      applicationService.storePausedMigrationConfig(any(), any(), any())
    } returns deliveryConfig

    coEvery {
      applicationService.getActuationPlan(deliveryConfig)
    } returns ActuationPlan(
      application = deliveryConfig.application,
      timestamp = Instant.now(),
      environmentPlans = emptyList()
    )

    coEvery {
      deliveryConfigRepository.getApplicationMigrationStatus(any())
    } returns ApplicationMigrationStatus(
      exportSucceeded = true,
      inAllowList = true,
      autoGeneratedConfig = mapper.convertValue(deliveryConfig, Map::class.java) as Map<String, Any?>,
      prLink = "https://link-to-pr",
      userGeneratedConfig = UserGeneratedConfig(
        config = mapper.convertValue(deliveryConfig, Map::class.java) as Map<String, Any?>,
        updatedBy = "user",
        updatedAt = clock.instant()
      )
    )
  }

  fun getQuery(path: String) = javaClass.getResource(path).readText().trimIndent()

  fun getHeaders(): HttpHeaders {
    val headers = HttpHeaders()
    headers.add("X-SPINNAKER-USER", user)
    return headers
  }

  @Test
  fun basicTest() {
    expectCatching {
      dgsQueryExecutor.executeAndExtractJsonPath<String>(
        getQuery("/dgs/basicQuery.graphql"),
        "data.md_application.environments[0].name",
        mapOf("appName" to "fnord")
      )
    }.isSuccess().isEqualTo("test")
  }

  @Test
  fun jsonSchema() {
    expectCatching {
      dgsQueryExecutor.executeAndExtractJsonPath<Map<String, Any>?>(
        getQuery("/dgs/jsonSchema.graphql"),
        "data.md_jsonSchema.schema"
      )
    }.isSuccess().isNotNull()
  }


  private fun fetchWritePermissions() = expectCatching {
    dgsQueryExecutor.executeAndExtractJsonPathAsObject(
      getQuery("/dgs/writePermissions.graphql"),
      "data.md_application.userPermissions",
      mapOf("appName" to "fnord"),
      MD_UserPermissions::class.java,
      getHeaders()
    )
  }

  @Test
  fun `user has write permissions`() {
    fetchWritePermissions().isSuccess().and {
      get { writeAccess }.isEqualTo(true)
      get { error }.isNull()
    }
  }

  @Test
  fun `user does not have write access to service account`() {
    every {
      authorizationSupport.hasServiceAccountAccess(any())
    } returns false

    coEvery {
      front50Service.getManuallyCreatedServiceAccounts(any())
    } returns emptyList()

    fetchWritePermissions().isSuccess().and {
      get { writeAccess }.isEqualTo(false)
      get { error }.isNotNull().contains("Service account was automatically created")
    }
  }

  @Test
  fun `user does not have write access to all groups`() {
    every {
      authorizationSupport.hasServiceAccountAccess(any())
    } returns false

    val memberOf = "managed-delivery@spinnaker.io"

    coEvery {
      front50Service.getManuallyCreatedServiceAccounts(any())
    } returns listOf(
      ServiceAccount(
        name = deliveryConfig.serviceAccount,
        memberOf = listOf(memberOf),
        lastModified = null,
        lastModifiedBy = null,
      )
    )

    fetchWritePermissions().isSuccess().and {
      get { writeAccess }.isEqualTo(false)
      get { error }.isNotNull()
        .contains("User must have access to all the groups of service account ${deliveryConfig.serviceAccount}: [$memberOf]")
    }
  }

  @Test
  fun artifactVersionStatus() {
    expectCatching {
      dgsQueryExecutor.executeAndExtractJsonPath<String>(
        getQuery("/dgs/basicQuery.graphql"),
        "data.md_application.environments[0].state.artifacts[0].versions[0].status",
        mapOf("appName" to "fnord")
      )
    }.isSuccess().isEqualTo("CURRENT")
  }

  @Test
  fun versionOnUnpinning() {
    expectCatching {
      dgsQueryExecutor.executeAndExtractJsonPath<String>(
        getQuery("/dgs/pinningAndRollback.graphql"),
        "data.md_application.versionOnUnpinning.version",
        mapOf("appName" to "fnord", "reference" to artifact.reference, "environment" to "test")
      )
    }.isSuccess().isEqualTo("v123")
  }

  @Test
  fun rollbackToVersion() {
    val pinSlot = slot<EnvironmentArtifactPin>()
    val markAsBadSlot = slot<EnvironmentArtifactVeto>()

    expectCatching {
      dgsQueryExecutor.executeAndGetDocumentContext(
        getQuery("/dgs/rollbackTo.graphql"),
        mapOf("payload" to mapOf(
          "application" to "fnord",
          "reference" to "fnord",
          "environment" to "test",
          "fromVersion" to "v2",
          "toVersion" to "v1",
          "comment" to "bad version"
        )),
        getHeaders()
      )
    }.isSuccess()

    verify {
      applicationService.pin(any(), "fnord", capture(pinSlot))
    }

    verify {
      applicationService.markAsVetoedIn(any(), "fnord", capture(markAsBadSlot), any())
    }

    expectThat(pinSlot.captured.version).isEqualTo("v1")
    expectThat(pinSlot.captured.type).isEqualTo(PinType.ROLLBACK)
    expectThat(markAsBadSlot.captured.version).isEqualTo("v2")
  }

  @Test
  fun `initiate migration - stored config`() {
    coEvery {
      artifactRepository.getCurrentlyDeployedArtifactVersionId(any(), any(), any())
    } returns null

    expectCatching {
      dgsQueryExecutor.executeAndExtractJsonPathAsObject(
        getQuery("/dgs/initiateMigration.graphql"),
        "data.md_initiateApplicationMigration",
        mapOf("payload" to mapOf("application" to deliveryConfig.application)),
        MD_Migration::class.java,
        getHeaders()
      )
    }.isSuccess().and {
      get { status }.isEqualTo(MD_MigrationStatus.PR_CREATED)
      get { actuationPlan }.isNotNull()
        .get { status }.isEqualTo(MD_ActuationPlanStatus.PENDING)
    }

    coVerify { applicationService.openMigrationPr(deliveryConfig.application, user, any()) }
    coVerify {
      applicationService.storePausedMigrationConfig(
        deliveryConfig.application,
        user,
        submittedDeliveryConfig
      )
    }
  }

  @Test
  fun `initiate migration - submit a delivery config`() {
    coEvery {
      artifactRepository.getCurrentlyDeployedArtifactVersionId(any(), any(), any())
    } returns null

    val configAsMap = mapper.convertValue(deliveryConfig, Map::class.java) as Map<String, Any>

    expectCatching {
      dgsQueryExecutor.executeAndExtractJsonPathAsObject(
        getQuery("/dgs/initiateMigration.graphql"),
        "data.md_initiateApplicationMigration",
        mapOf(
          "payload" to mapOf(
            "application" to deliveryConfig.application,
            "deliveryConfig" to configAsMap
          )
        ),
        MD_Migration::class.java,
        getHeaders()
      )
    }.isSuccess().and {
      get { status }.isEqualTo(MD_MigrationStatus.PR_CREATED)
      get { actuationPlan }.isNotNull()
        .get { status }.isEqualTo(MD_ActuationPlanStatus.PENDING)
    }

    coVerify { applicationService.openMigrationPr(deliveryConfig.application, user, configAsMap) }
    coVerify { applicationService.storePausedMigrationConfig(deliveryConfig.application, user, submittedDeliveryConfig) }
  }

  @Test
  fun `valid delivery config`() {
    val configAsMap = mapper.convertValue(deliveryConfig, Map::class.java)

    expectCatching {
      dgsQueryExecutor.executeAndExtractJsonPathAsObject(
        getQuery("/dgs/validateConfig.graphql"),
        "data.md_validateDeliveryConfig",
        mapOf("payload" to mapOf("deliveryConfig" to configAsMap)),
        MD_ValidateResult::class.java,
        getHeaders()
      )
    }.isSuccess().and {
      get { success }.isTrue()
      get { errors }.isNull()
    }
  }

  @Test
  fun `invalid delivery config`() {
    every {
      applicationService.parseAndValidateDeliveryConfig(any())
    } throws InvalidAppNameException("App name is missing")

    val configAsMap = mapper.convertValue(deliveryConfig, Map::class.java).toMutableMap().remove("application")

    expectCatching {
      dgsQueryExecutor.executeAndExtractJsonPathAsObject(
        getQuery("/dgs/validateConfig.graphql"),
        "data.md_validateDeliveryConfig",
        mapOf("payload" to mapOf("deliveryConfig" to configAsMap)),
        MD_ValidateResult::class.java,
        getHeaders()
      )
    }.isSuccess().and {
      get { success }.isFalse()
      get { errors }.isNotNull()
    }
  }

  @Test
  fun checkMigrationStatus() {
    coEvery {
      artifactRepository.getCurrentlyDeployedArtifactVersionId(any(), any(), any())
    } returns CurrentlyDeployedVersion("version", clock.instant())

    expectCatching {
      dgsQueryExecutor.executeAndExtractJsonPathAsObject(
        getQuery("/dgs/migrationStatus.graphql"),
        "data.md_migration",
        mapOf("appName" to deliveryConfig.application),
        MD_Migration::class.java,
        getHeaders()
      )
    }.isSuccess().and {
      get { status }.isEqualTo(MD_MigrationStatus.PR_CREATED)
      get { actuationPlan }.isNotNull()
        .get { status }.isEqualTo(MD_ActuationPlanStatus.COMPLETED)
      get { userGeneratedConfig }.isNotNull()
        .get { deliveryConfig }.isA<Map<String, Any>>()
    }
  }

  @Test
  fun checkAutoSaveWithNoConfig() {
    expectCatching {
      dgsQueryExecutor.executeAndExtractJsonPathAsObject(
        getQuery("/dgs/autoSaveUserGeneratedConfig.graphql"),
        "data.md_autoSaveMigrationConfig",
        mapOf(
          "payload" to mapOf(
            "application" to deliveryConfig.application
          ),
        ),
        Boolean::class.java,
        getHeaders()
      )
    }.isSuccess().get {
     this
    }.isFalse()
  }

  @Test
  fun checkAutoSaveWithValidConfig() {
    every {
        applicationService.storeUserGeneratedConfig(any(), any(), any())
    } just runs

    val configAsMap = mapper.convertValue(deliveryConfig, Map::class.java)

    expectCatching {
      dgsQueryExecutor.executeAndExtractJsonPathAsObject(
        getQuery("/dgs/autoSaveUserGeneratedConfig.graphql"),
        "data.md_autoSaveMigrationConfig",
        mapOf(
          "payload" to mapOf(
            "application" to deliveryConfig.application,
            "deliveryConfig" to configAsMap
          ),
        ),
        Boolean::class.java,
        getHeaders()
      )
    }.isSuccess()
      .get {
        this
      }.isTrue()

  }
}
