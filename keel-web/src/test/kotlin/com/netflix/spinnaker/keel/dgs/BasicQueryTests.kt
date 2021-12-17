package com.netflix.spinnaker.keel.dgs

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.graphql.dgs.DgsQueryExecutor
import com.netflix.graphql.dgs.autoconfig.DgsAutoConfiguration
import com.netflix.spinnaker.keel.actuation.ExecutionSummaryService
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.SubnetAwareRegionSpec
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.Repo
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.ec2.EC2_CLUSTER_V1_1
import com.netflix.spinnaker.keel.artifacts.ArtifactVersionLinks
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.auth.AuthorizationSupport
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactPin
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVeto
import com.netflix.spinnaker.keel.core.api.PinType
import com.netflix.spinnaker.keel.core.api.PromotionStatus
import com.netflix.spinnaker.keel.core.api.PublishedArtifactInEnvironment
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.Front50Service
import com.netflix.spinnaker.keel.graphql.types.MD_UserPermissions
import com.netflix.spinnaker.keel.igor.DeliveryConfigImporter
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventRepository
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.persistence.DismissibleNotificationRepository
import com.netflix.spinnaker.keel.persistence.EnvironmentDeletionRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.TaskTrackingRepository
import com.netflix.spinnaker.keel.scm.ScmUtils
import com.netflix.spinnaker.keel.services.ApplicationService
import com.netflix.spinnaker.keel.services.ResourceStatusService
import com.netflix.spinnaker.keel.test.deliveryConfig
import com.netflix.spinnaker.keel.test.resource
import com.netflix.spinnaker.keel.upsert.DeliveryConfigUpserter
import com.netflix.spinnaker.keel.veto.unhappy.UnhappyVeto
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Runs
import io.mockk.coEvery
import io.mockk.every
import io.mockk.just
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.internal.stubbing.answers.Returns
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import strikt.assertions.isSuccess

@SpringBootTest(
  classes = [DgsAutoConfiguration::class, DgsTestConfig::class],
)
class BasicQueryTests {

  @Autowired
  lateinit var dgsQueryExecutor: DgsQueryExecutor

  @MockkBean
  lateinit var authorizationSupport: AuthorizationSupport

  @MockkBean
  lateinit var keelRepository: KeelRepository

  @MockkBean
  lateinit var actuationPauser: ActuationPauser

  @MockkBean
  lateinit var artifactVersionLinks: ArtifactVersionLinks

  @MockkBean
  lateinit var notificationRepository: DismissibleNotificationRepository

  @MockkBean
  lateinit var scmUtils: ScmUtils

  @MockkBean
  lateinit var executionSummaryService: ExecutionSummaryService

  @MockkBean
  lateinit var yamlMapper: YAMLMapper

  @MockkBean
  lateinit var deliveryConfigImporter: DeliveryConfigImporter

  @MockkBean
  lateinit var environmentDeletionRepository: EnvironmentDeletionRepository

  @MockkBean
  lateinit var front50Service: Front50Service

  @MockkBean
  lateinit var front50Cache: Front50Cache

  @MockkBean
  lateinit var deliveryConfigUpserter: DeliveryConfigUpserter

  @MockkBean
  lateinit var lifecycleEventRepository: LifecycleEventRepository

  @MockkBean
  lateinit var applicationService: ApplicationService

  @MockkBean
  lateinit var unhappyVeto: UnhappyVeto

  @MockkBean
  lateinit var deliveryConfigRepository: DeliveryConfigRepository

  @MockkBean
  lateinit var resourceStatusService: ResourceStatusService

  @MockkBean
  lateinit var taskTrackingRepository: TaskTrackingRepository

  private val artifact = DebianArtifact(
    name = "fnord",
    deliveryConfigName = "fnord",
    vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2"))
  )

  private val resource = resource(
    kind = EC2_CLUSTER_V1_1.kind,
    spec = ClusterSpec(
      moniker = Moniker("fnord"),
      artifactReference = "fnord",
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

  private val artifactVersion = PublishedArtifact(
    name = artifact.name,
    reference = artifact.reference,
    type = artifact.type,
    version = "v123",
    gitMetadata = GitMetadata(
      commit = "abc123",
      author = "emburns",
      project = "spkr",
      branch = "main",
      repo = Repo(name = "keel"),
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

  private val deliveryConfig = deliveryConfig(artifact = artifact, resources = setOf(resource))

  @BeforeEach
  fun setup() {
    every {
      keelRepository.getDeliveryConfigForApplication(any())
    } returns deliveryConfig

    every {
      keelRepository.getAllVersionsForEnvironment(artifact, deliveryConfig, "test")
    } returns listOf(
      PublishedArtifactInEnvironment(
        artifact.toArtifactVersion(version = "v1"),
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
      authorizationSupport.hasApplicationPermission("WRITE", "APPLICATION", any())
    } returns true

    every {
      authorizationSupport.hasServiceAccountAccess(any())
    } returns true
  }

  fun getQuery(path: String) = javaClass.getResource(path).readText().trimIndent()

  fun getHeaders(): HttpHeaders {
    val headers = HttpHeaders()
    headers.add("X-SPINNAKER-USER", "userName")
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
      get { error }.isNotNull()
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
        mapOf("appName" to "fnord", "reference" to "fnord", "environment" to "test")
      )
    }.isSuccess().isEqualTo("v123")
  }

  @Test
  fun rollbackToVersion() {
    var pinSlot = slot<EnvironmentArtifactPin>()
    var markAsBadSlot = slot<EnvironmentArtifactVeto>()

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
}
