package com.netflix.spinnaker.keel.export

import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spinnaker.config.BaseUrlConfig
import com.netflix.spinnaker.keel.api.TaskStatus
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.ec2.EC2_CLASSIC_LOAD_BALANCER_V1
import com.netflix.spinnaker.keel.api.ec2.EC2_CLUSTER_V1_1
import com.netflix.spinnaker.keel.api.ec2.EC2_SECURITY_GROUP_V1
import com.netflix.spinnaker.keel.api.migration.SkipReason
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.model.Credential
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.core.api.SubmittedResource
import com.netflix.spinnaker.keel.ec2.resource.ClassicLoadBalancerHandler
import com.netflix.spinnaker.keel.ec2.resource.ClusterHandler
import com.netflix.spinnaker.keel.ec2.resource.SecurityGroupHandler
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.model.Application
import com.netflix.spinnaker.keel.front50.model.Cluster
import com.netflix.spinnaker.keel.front50.model.DeployStage
import com.netflix.spinnaker.keel.front50.model.Pipeline
import com.netflix.spinnaker.keel.front50.model.PipelineNotifications
import com.netflix.spinnaker.keel.front50.model.RestrictedExecutionWindow
import com.netflix.spinnaker.keel.front50.model.SlackChannel
import com.netflix.spinnaker.keel.front50.model.TimeWindowConfig
import com.netflix.spinnaker.keel.front50.model.Trigger
import com.netflix.spinnaker.keel.igor.JobService
import com.netflix.spinnaker.keel.orca.ExecutionDetailResponse
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.persistence.NoDeliveryConfigForApplication
import com.netflix.spinnaker.keel.test.classicLoadBalancer
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import com.netflix.spinnaker.keel.test.ec2Cluster
import com.netflix.spinnaker.keel.test.mockEnvironment
import com.netflix.spinnaker.keel.test.securityGroup
import com.netflix.spinnaker.keel.test.submittedDeliveryConfig
import com.netflix.spinnaker.keel.test.titusCluster
import com.netflix.spinnaker.keel.titus.TitusClusterHandler
import com.netflix.spinnaker.keel.validators.DeliveryConfigValidator
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.first
import strikt.assertions.hasSize
import strikt.assertions.isA
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isTrue
import java.time.Instant
import io.mockk.coEvery as every
import io.mockk.coVerify as verify

internal class ExportServiceTests {
  private val front50Cache: Front50Cache = mockk()
  private val orcaService: OrcaService = mockk()
  private val yamlMapper: YAMLMapper = mockk()
  private val deliveryConfigRepository: DeliveryConfigRepository = mockk()
  private val jobService: JobService = mockk()
  private val cloudDriverCache: CloudDriverCache = mockk()
  private val titusClusterHandler: TitusClusterHandler = mockk()
  private val ec2ClusterHandler: ClusterHandler = mockk()
  private val securityGroupHandler: SecurityGroupHandler = mockk()
  private val classicLbHandler: ClassicLoadBalancerHandler = mockk()
  private val objectMapper = configuredTestObjectMapper()
  private val prettyPrinter: ObjectWriter = mockk()

  private val subject = ExportService(
    handlers = listOf(titusClusterHandler, ec2ClusterHandler, securityGroupHandler, classicLbHandler),
    front50Cache = front50Cache,
    orcaService = orcaService,
    baseUrlConfig = BaseUrlConfig(),
    yamlMapper = yamlMapper,
    validator = DeliveryConfigValidator(),
    deliveryConfigRepository = deliveryConfigRepository,
    springEnv = mockEnvironment(),
    cloudDriverCache = cloudDriverCache
  )

  private val submittedDeliveryCofig = submittedDeliveryConfig()
  private val appName = "fnord"
  private val pipelineSlackChannel = "great-pipeline-channel"
  private val secondPipelineSlackChannel = "second-great-pipeline-channel"
  private val appSlackChannel = "great-app-channel"

  private val pipeline = Pipeline(
    name = "pipeline",
    id = "1",
    application = appName,
  )

  private val pipelineWithNotifications = Pipeline(
    name = "awesome-pipeline",
    id = "2",
    application = appName,
    notifications = listOf(
      PipelineNotifications(
        address = pipelineSlackChannel,
        type = "slack"
      ),
      PipelineNotifications(
        address = secondPipelineSlackChannel,
        type = "slack"
      ),
      PipelineNotifications(
        address = "keel@email",
        type = "email"
      )
    ),
    disabled = false,
    fromTemplate = false,
    triggers = listOf(Trigger(
      type = "jenkins",
      enabled = true,
      application = null,
      pipeline = null)),
    _stages = listOf(
      DeployStage(
        name = "Deploy",
        type = "deploy",
        refId = "2",
        requisiteStageRefIds = listOf("1"),
        clusters = setOf(Cluster(
          account = "test",
          application = appName,
          provider = "aws",
          strategy = "highlander",
          availabilityZones = mapOf("us-east-1" to listOf("us-east-1c", "us-east-1d", "us-east-1e")),
          _region = null)))),
    _updateTs = 1645036222851,
    lastModifiedBy = "md@netflix.com"
  )

  private val pipelineWithProdEnvAndNotifications = pipelineWithNotifications.copy(
    _stages = listOf(DeployStage(
      name = "Deploy",
      type = "deploy",
      refId = "2",
      requisiteStageRefIds = listOf("1"),
      clusters = setOf(Cluster(
        account = "prod",
        application = appName,
        provider = "aws",
        strategy = "highlander",
        availabilityZones = mapOf("us-east-1" to listOf("us-east-1c", "us-east-1d", "us-east-1e")),
        _region = null)))),
    )


  private val lastExecution = ExecutionDetailResponse(
    id = "123",
    name = "aws-deploy-to-test",
    application = "fnord",
    buildTime = Instant.now(),
    status = TaskStatus.SUCCEEDED,
    startTime = null,
    endTime = null
  )

  private val restrictedExecutionWindow = RestrictedExecutionWindow(
    whitelist = listOf(
      TimeWindowConfig(startHour = 2, startMin = 0, endHour = 5, endMin = 0),
      TimeWindowConfig(startHour = 10, startMin = 0, endHour = 19, endMin = 0)),
    days = listOf(2, 3, 4, 5, 6)
  )

  private val exportResult = PipelineExportResult(
    deliveryConfig = submittedDeliveryCofig,
    baseUrl = "https://baseurl.com",
    exported = mapOf(pipeline to submittedDeliveryCofig.environments.toList()),
    projectKey = "spkr",
    repoSlug = "keel",
    configValidationException = null,
    skipped = emptyMap()
  )

  private val titusCluster = titusCluster()
  private val ec2Cluster = ec2Cluster()

  private val testAccount =
    Credential("test", "titus", "test",
      mutableMapOf("regions" to titusCluster.spec.locations.regions.map {
        mapOf("name" to it.name)
      })
    )

  private val artifact = DebianArtifact(
    appName,
    reference = appName,
    vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2"))
  )


  @BeforeEach
  fun setup() {
    every {
      front50Cache.applicationByName(appName)
    } returns Application(name = appName, repoType = "stash", repoProjectKey = "spkr", repoSlug = appName)

    every {
      deliveryConfigRepository.updateMigratingAppScmStatus("fnord", any())
    } just runs

    every {
      titusClusterHandler.supportedKind
    } returns TITUS_CLUSTER_V1

    every {
      ec2ClusterHandler.supportedKind
    } returns EC2_CLUSTER_V1_1

    every {
      classicLbHandler.supportedKind
    } returns EC2_CLASSIC_LOAD_BALANCER_V1

    every {
      titusClusterHandler.export(any())
    } returns titusCluster.spec

    every {
      ec2ClusterHandler.export(any())
    } returns ec2Cluster.spec

    every {
      securityGroupHandler.export(any())
    } returns securityGroup().spec

    every {
      classicLbHandler.export(any())
    } returns classicLoadBalancer().spec

    every {
      cloudDriverCache.credentialBy(any())
    } returns testAccount

    every {
      securityGroupHandler.supportedKind
    } returns EC2_SECURITY_GROUP_V1

    every {
      prettyPrinter.writeValueAsString(any())
    } returns "hello"

    every {
      yamlMapper.writerWithDefaultPrettyPrinter()
    } returns prettyPrinter

    every {
      deliveryConfigRepository.getByApplication(any())
    } throws NoDeliveryConfigForApplication("fnord")

    every {
      front50Cache.pipelinesByApplication("fnord")
    } returns listOf(pipelineWithNotifications)

    every {
      orcaService.getExecutions(any())
    } returns listOf(lastExecution)

    every { ec2ClusterHandler.exportArtifact(any()) } returns artifact

    objectMapper.registerSubtypes(NamedType(TitusClusterSpec::class.java, TITUS_CLUSTER_V1.kind.toString()))
  }


  @Test
  fun `don't set slack notification for non-production environments`() {
    val result = runBlocking {
      subject.exportFromPipelines(appName)
    }

    expectThat(result) {
      isA<PipelineExportResult>().and {
        get { deliveryConfig.environments }
          .first()
          .get { notifications }
          .isEmpty()
      }
    }
  }

  @Test
  fun `set notification for production environment`() {
    every {
      front50Cache.pipelinesByApplication(appName)
    } returns listOf(pipelineWithProdEnvAndNotifications)

    val differentResults = runBlocking {
      subject.exportFromPipelines(appName)
    }

    expectThat(differentResults) {
      isA<PipelineExportResult>().and {
        get { deliveryConfig.environments }
          .first()
          .get { notifications }
          .hasSize(2)
          .first()
          .get { address }.isEqualTo(pipelineSlackChannel)
      }
    }
  }

  @Test
  fun `application's notifications takes precedents`() {
    every {
      front50Cache.pipelinesByApplication(appName)
    } returns listOf(pipelineWithProdEnvAndNotifications)

    every {
      front50Cache.applicationByName(appName)
    } returns Application(name = appName, repoType = "stash", repoProjectKey = "spkr", repoSlug = appName, slackChannel = SlackChannel(appSlackChannel))

    val differentResults = runBlocking {
      subject.exportFromPipelines(appName)
    }

    expectThat(differentResults) {
      isA<PipelineExportResult>().and {
        get { deliveryConfig.environments }
          .first()
          .get { notifications }
          .first()
          .get { address }.isEqualTo(appSlackChannel)
      }
    }
  }


  @Test
  fun `export is successful`() {
    expectThat(exportResult.exportSucceeded).isTrue()
  }

  @Test
  fun `export is successful if only has old and disabled pipelines`() {
    listOf(SkipReason.DISABLED, SkipReason.NOT_EXECUTED_RECENTLY).forEach {
      expectThat(exportResult.copy(skipped = mapOf(pipeline to it)).exportSucceeded).isTrue()
    }
  }

  @Test
  fun `export is not successful if some pipelines failed to export`() {
    listOf(SkipReason.SHAPE_NOT_SUPPORTED, SkipReason.HAS_PARALLEL_STAGES, SkipReason.FROM_TEMPLATE).forEach {
      expectThat(exportResult.copy(skipped = mapOf(pipeline to it)).exportSucceeded).isFalse()
    }
  }

  @Test
  fun `exporting a resource delegates to the correct handler`() {
    val exported = runBlocking {
      subject.exportResource("titus", "cluster", titusCluster.spec.locations.account, titusCluster.name, "keel@keel.io")
    }
    verify { titusClusterHandler.export(any()) }
    expectThat(exported.spec).isEqualTo(titusCluster.spec)
  }

  @Test
  fun `re-exporting a resource delegates to the correct handler`() {
    val submittedResource = objectMapper.convertValue<SubmittedResource<TitusClusterSpec>>(titusCluster)
    val exported = runBlocking {
      subject.reExportResource(submittedResource, "keel@keel.io")
    }
    verify { titusClusterHandler.export(any()) }
    expectThat(exported.spec).isEqualTo(titusCluster.spec)
  }

}
