package com.netflix.spinnaker.keel.export

import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spinnaker.config.BaseUrlConfig
import com.netflix.spinnaker.keel.api.Application
import com.netflix.spinnaker.keel.api.Exportable
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.SlackChannel
import com.netflix.spinnaker.keel.api.SubnetAwareRegionSpec
import com.netflix.spinnaker.keel.api.TaskStatus.SUCCEEDED
import com.netflix.spinnaker.keel.api.artifacts.ArtifactOriginFilter
import com.netflix.spinnaker.keel.api.artifacts.branchName
import com.netflix.spinnaker.keel.api.ec2.ApplicationLoadBalancerSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterDependencies
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.ec2.EC2_APPLICATION_LOAD_BALANCER_V1_2
import com.netflix.spinnaker.keel.api.ec2.EC2_CLASSIC_LOAD_BALANCER_V1
import com.netflix.spinnaker.keel.api.ec2.EC2_CLUSTER_V1_1
import com.netflix.spinnaker.keel.api.ec2.EC2_SECURITY_GROUP_V1
import com.netflix.spinnaker.keel.api.ec2.LaunchConfigurationSpec
import com.netflix.spinnaker.keel.api.ec2.SecurityGroupSpec
import com.netflix.spinnaker.keel.api.migration.SkipReason
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.api.titus.TitusServerGroupSpec
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.clouddriver.model.ApplicationLoadBalancerModel
import com.netflix.spinnaker.keel.clouddriver.model.Credential
import com.netflix.spinnaker.keel.core.api.AllowedTimesConstraint
import com.netflix.spinnaker.keel.core.api.SubmittedResource
import com.netflix.spinnaker.keel.ec2.resource.ApplicationLoadBalancerHandler
import com.netflix.spinnaker.keel.ec2.resource.ClassicLoadBalancerHandler
import com.netflix.spinnaker.keel.ec2.resource.ClusterHandler
import com.netflix.spinnaker.keel.ec2.resource.SecurityGroupHandler
import com.netflix.spinnaker.keel.export.ExportService.Companion.EXPORTABLE_PIPELINE_SHAPES
import com.netflix.spinnaker.keel.export.canary.CanaryConstraint
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.model.Cluster
import com.netflix.spinnaker.keel.front50.model.DeployStage
import com.netflix.spinnaker.keel.front50.model.GenericStage
import com.netflix.spinnaker.keel.front50.model.JenkinsStage
import com.netflix.spinnaker.keel.front50.model.Pipeline
import com.netflix.spinnaker.keel.front50.model.PipelineNotifications
import com.netflix.spinnaker.keel.front50.model.RestrictedExecutionWindow
import com.netflix.spinnaker.keel.front50.model.TimeWindowConfig
import com.netflix.spinnaker.keel.front50.model.Trigger
import com.netflix.spinnaker.keel.jenkins.BasicReport
import com.netflix.spinnaker.keel.jenkins.JUnitReportConfig
import com.netflix.spinnaker.keel.jenkins.JenkinsProject
import com.netflix.spinnaker.keel.jenkins.JenkinsService
import com.netflix.spinnaker.keel.jenkins.JobConfig
import com.netflix.spinnaker.keel.notifications.slack.SlackService
import com.netflix.spinnaker.keel.orca.ExecutionDetailResponse
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.persistence.NoDeliveryConfigForApplication
import com.netflix.spinnaker.keel.test.applicationLoadBalancer
import com.netflix.spinnaker.keel.test.classicLoadBalancer
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import com.netflix.spinnaker.keel.test.debianArtifact
import com.netflix.spinnaker.keel.test.ec2Cluster
import com.netflix.spinnaker.keel.test.mockEnvironment
import com.netflix.spinnaker.keel.test.securityGroup
import com.netflix.spinnaker.keel.test.submittedDeliveryConfig
import com.netflix.spinnaker.keel.test.titusCluster
import com.netflix.spinnaker.keel.titus.TitusClusterHandler
import com.netflix.spinnaker.keel.validators.DeliveryConfigValidator
import com.netflix.spinnaker.keel.verification.jenkins.JenkinsJobVerification
import com.netflix.spinnaker.keel.veto.unhealthy.UnsupportedResourceTypeException
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.contains
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.first
import strikt.assertions.hasSize
import strikt.assertions.isA
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isTrue
import strikt.mockk.captured
import java.time.Instant
import io.mockk.coEvery as every
import io.mockk.coVerify as verify

internal class ExportServiceTests {
  private val front50Cache: Front50Cache = mockk()
  private val orcaService: OrcaService = mockk()
  private val yamlMapper: YAMLMapper = mockk()
  private val deliveryConfigRepository: DeliveryConfigRepository = mockk()
  private val cloudDriverCache: CloudDriverCache = mockk()
  private val titusClusterHandler: TitusClusterHandler = mockk()
  private val ec2ClusterHandler: ClusterHandler = mockk()
  private val securityGroupHandler: SecurityGroupHandler = mockk()
  private val classicLbHandler: ClassicLoadBalancerHandler = mockk()
  private val applicationLbHandler: ApplicationLoadBalancerHandler = mockk()
  private val objectMapper = configuredTestObjectMapper()
  private val jenkinsService: JenkinsService = mockk()
  private val slackService: SlackService = mockk()
  private val prettyPrinter: ObjectWriter = mockk()
  private val clouddriverService: CloudDriverService = mockk()


  private val subject = ExportService(
    handlers = listOf(titusClusterHandler, ec2ClusterHandler, securityGroupHandler, classicLbHandler, applicationLbHandler),
    front50Cache = front50Cache,
    orcaService = orcaService,
    baseUrlConfig = BaseUrlConfig(),
    yamlMapper = yamlMapper,
    validator = DeliveryConfigValidator(),
    deliveryConfigRepository = deliveryConfigRepository,
    springEnv = mockEnvironment(),
    cloudDriverCache = cloudDriverCache,
    jenkinsService = jenkinsService,
    slackService = slackService,
    slackNotificationChannel = "team-channel",
    cloudDriverService = clouddriverService
  )

  private val submittedDeliveryCofig = submittedDeliveryConfig()
  private val appName = "fnord"
  private val pipelineSlackChannel = "great-pipeline-channel"
  private val secondPipelineSlackChannel = "second-great-pipeline-channel"
  private val appSlackChannel = "great-app-channel"

  private val ec2Cluster = ec2Cluster().copy(
    spec = ec2Cluster().spec.copy(
      _defaults = ClusterSpec.ServerGroupSpec(
        launchConfiguration = LaunchConfigurationSpec(
          instanceType = "m5.large",
          ebsOptimized = true,
          iamRole = "fnordInstanceProfile",
          instanceMonitoring = false
        ),
        dependencies = ClusterDependencies(
          loadBalancerNames = setOf("fnord-internal"),
          securityGroupNames = setOf("fnord", "fnord-elb", "nf-somthing"),
          targetGroups = setOf("fnord-awesome")
        ),
      )
    )
  )

  private val titusCluster = titusCluster().copy(
    spec = titusCluster().spec.copy(
      _defaults = TitusServerGroupSpec(
        dependencies = ClusterDependencies(
          loadBalancerNames = setOf("fnord-internal"),
          securityGroupNames = setOf("fnord", "fnord-elb"),
          targetGroups = setOf("fnord-awesome")
        )
      )
    )
  )

  private val loadBalancer = ApplicationLoadBalancerModel(
    moniker = Moniker(ec2Cluster.application, "stub", "alb"),
    loadBalancerName = "fnord-test",
    dnsName = "stub-alb-1234567890.elb.amazonaws.com",
    targetGroups = listOf(
      ApplicationLoadBalancerModel.TargetGroup(
        targetGroupName = "fnord-awesome",
        loadBalancerNames = emptyList(),
        targetType = "whatever-this-is",
        matcher = ApplicationLoadBalancerModel.TargetGroupMatcher("200"),
        protocol = "https",
        port = 8080,
        healthCheckEnabled = true,
        healthCheckTimeoutSeconds = 30,
        healthCheckPort = "8080",
        healthCheckProtocol = "https",
        healthCheckPath = "/healthcheck",
        healthCheckIntervalSeconds = 60,
        healthyThresholdCount = 10,
        unhealthyThresholdCount = 5,
        vpcId = "vpc",
        attributes = ApplicationLoadBalancerModel.TargetGroupAttributes()
      )
    ),
    availabilityZones = setOf("a", "b", "c"),
    vpcId = "vpc",
    subnets = emptySet(),
    scheme = "https",
    idleTimeout = 60,
    securityGroups = emptySet(),
    listeners = emptyList(),
    ipAddressType = "v4"
  )

  private val pipeline = Pipeline(
    name = "pipeline",
    id = "1",
    application = appName,
  )

  private val region1 = "us-east-1"
  private val region2 = "us-west-2"
  private val region3 = "us-west-3"

  private val cluster = Cluster(
    account = "test",
    application = appName,
    provider = "aws",
    strategy = "highlander",
    availabilityZones = mapOf("us-east-1" to listOf("us-east-1c", "us-east-1d", "us-east-1e")),
    _region = region1
  )

  private val deployStage = DeployStage(
    name = "Deploy",
    refId = "2",
    requisiteStageRefIds = listOf("1"),
    clusters = setOf(cluster)
  )

  private val deployStageForTitus = DeployStage(
    name = "Deploy",
    refId = "2",
    requisiteStageRefIds = listOf("1"),
    clusters = setOf(
      Cluster(
        account = "titusprodvpc",
        application = appName,
        provider = "titus",
        strategy = "redblack",
        availabilityZones = mapOf("us-west-2" to listOf("us-east-1c", "us-east-1d", "us-east-1e")),
        _region = region2
      )
    )
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
      pipeline = null)
    ),
    _stages = listOf(deployStage),
    _updateTs = 1645036222851,
    lastModifiedBy = "md@netflix.com"
  )

  private val pipelineWithProdEnvAndNotifications = pipelineWithNotifications.copy(
    _stages = listOf(
      deployStage.run {
        copy(clusters = clusters.map { it.copy(account = "prod") }.toSet())
      }
    )
  )

  private val restrictedExecutionWindow = RestrictedExecutionWindow(
    whitelist = listOf(
      TimeWindowConfig(startHour = 2, startMin = 0, endHour = 5, endMin = 0),
      TimeWindowConfig(startHour = 10, startMin = 0, endHour = 19, endMin = 0)),
    days = listOf(2, 3, 4, 5, 6)
  )

  private val pipelineWithTwoDeployDifferentResources = pipelineWithNotifications.copy(
    _stages = listOf(
      deployStage.copy(
        restrictedExecutionWindow = restrictedExecutionWindow,
      ),
      deployStageForTitus
    )
  )

  private val pipelineWithTwoDeployDifferentRegions = pipelineWithNotifications.copy(
    _stages = listOf(
      deployStage.copy(
        restrictedExecutionWindow = restrictedExecutionWindow,
      ),
      deployStage.copy(
        clusters = setOf(cluster.copy(
          _region = region2,
          availabilityZones = emptyMap()
        ),
          cluster.copy(
          _region = region3,
          availabilityZones = emptyMap()
        )
        )
      )
    )
  )

  private val pipelineWithCanary = pipelineWithNotifications.copy(
    _stages = listOf(
      deployStage,
      canaryStage
    )
  )

  private val jenkinsStageWithTests = JenkinsStage(
    name = "Integration tests",
    refId = "2",
    requisiteStageRefIds = listOf("1"),
    controller = "ctrl",
    job = "job",
    parameters = mapOf(
      "param1" to "val1",
      "param2" to "val2"
    )
  )

  private val pipelineWithJenkinsTests = Pipeline(
    name = "pipeline-with-jenkins-tests",
    id = "1",
    application = appName,
    _stages = listOf(
      deployStage,
      jenkinsStageWithTests
    ),
    _updateTs = 1645036222851,
    lastModifiedBy = "md@netflix.com"
  )

  private val pipelineWithTitus = Pipeline(
    name = "pipeline-with-titus",
    id = "1",
    application = appName,
    _stages = listOf(
      deployStageForTitus,
    ),
    _updateTs = 1645036222851,
    lastModifiedBy = "md@netflix.com"
  )

  private val jenkinsJobWithTests = JobConfig(
    JenkinsProject(
      publishers = listOf(
        JUnitReportConfig(listOf(BasicReport("report", ".", "test-report.xml")))
      )
    )
  )

  private val jenkinsJobWithoutTests = JobConfig(JenkinsProject())

  private val lastExecution = ExecutionDetailResponse(
    id = "123",
    name = "aws-deploy-to-test",
    application = appName,
    buildTime = Instant.now(),
    status = SUCCEEDED,
    startTime = null,
    endTime = null
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

  private val testAccount =
    Credential("test", "titus", "test",
      mutableMapOf("regions" to titusCluster.spec.locations.regions.map {
        mapOf("name" to it.name)
      })
    )

  private val artifact = debianArtifact(appName)

  private val dockerArtifact = DockerArtifact(
    name = "org/$appName",
    reference = "$appName-docker",
    from = ArtifactOriginFilter(branchName("main")),
    metadata = emptyMap()
  )


  @BeforeEach
  fun setup() {
    every {
      front50Cache.applicationByName(appName)
    } returns Application(name = appName, repoType = "stash", repoProjectKey = "spkr", repoSlug = appName)

    every {
      applicationLbHandler.supportedKind
    } returns EC2_APPLICATION_LOAD_BALANCER_V1_2

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
    } answers {
      ec2Cluster.spec.copy(locations = ec2Cluster.spec.locations.copy(regions = firstArg<Exportable>().regions.map {
        SubnetAwareRegionSpec(
          name = it
        )
      }.toSet()))
    }

    every {
      securityGroupHandler.export(any())
    } returns securityGroup().spec

    every {
      classicLbHandler.export(any())
    } returns classicLoadBalancer().spec

    every {
      applicationLbHandler.export(any())
    } returns applicationLoadBalancer().spec

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
    } throws NoDeliveryConfigForApplication(appName)

    every {
      front50Cache.pipelinesByApplication(appName)
    } returns listOf(pipelineWithNotifications)

    every {
      orcaService.getExecutions(any())
    } returns listOf(lastExecution)

    every { ec2ClusterHandler.exportArtifact(any()) } returns artifact

    every {
      slackService.postChatMessage(any(), any(), any())
    } returns mockk()

    every {
      clouddriverService.loadBalancersForApplication(any(), any())
    } returns listOf(loadBalancer)

    objectMapper.registerSubtypes(NamedType(TitusClusterSpec::class.java, TITUS_CLUSTER_V1.kind.toString()))
  }

  @Test
  fun `set notifications if we can't infer the production environments`() {
    val result = runBlocking {
      subject.exportFromPipelines(appName)
    }

    expectThat(result) {
      isA<PipelineExportResult>().and {
        get { deliveryConfig.environments }
          .hasSize(1)
          .first()
          .get { notifications }
          .first()
          .get { address }.isEqualTo(pipelineSlackChannel)
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
  fun `application's notifications takes precedence`() {
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
  fun `application is active`() {
    expectThat(exportResult.isInactive).isFalse()
  }

  @Test
  fun `application is inactive`() {
    val result = PipelineExportResult(
      deliveryConfig = submittedDeliveryCofig.copy(environments = emptySet()),
      baseUrl = "https://baseurl.com",
      exported = emptyMap(),
      projectKey = "spkr",
      repoSlug = "keel",
      configValidationException = null,
      skipped = mapOf(pipeline to SkipReason.NOT_EXECUTED_RECENTLY)
    )
    expectThat(result.isInactive).isTrue()
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

  @Test
  fun `basic pipeline shapes are exportable`() {
    EXPORTABLE_PIPELINE_SHAPES.forEach { shape ->
      val pipeline = Pipeline(
        name = "pipeline",
        id = "1",
        application = appName,
        _stages = shape.mapIndexed { index, stage -> GenericStage(name = "whatever", type = stage, refId = "$index") }
      )
      expectThat(subject.isExportable(pipeline, includeVerifications = false)).isTrue()
    }
  }

  @Test
  fun `pipeline shapes with added jenkins stage are only exportable when exporting verifications is enabled`() {
    fun jenkinsStage(deployIndex: Int) = GenericStage(name = "jenkins", type = "jenkins", refId = "${deployIndex + 1}")

    EXPORTABLE_PIPELINE_SHAPES.forEach { shape ->
      var deployIndex = -1
      var pipeline = Pipeline(
        name = "pipeline",
        id = "1",
        application = appName,
        _stages = shape.mapIndexed { index, stage ->
          if (stage == "deploy") deployIndex = index
          GenericStage(name = "whatever", type = stage, refId = "$index")
        }
      )
      val stagesWithJenkins = pipeline.stages.toMutableList().apply { add(deployIndex + 1, jenkinsStage(deployIndex)) }
      pipeline = pipeline.copy(_stages = stagesWithJenkins)
      expectThat(subject.isExportable(pipeline, includeVerifications = false)).isFalse()
      expectThat(subject.isExportable(pipeline, includeVerifications = true)).isTrue()
    }
  }

  @Test
  fun `pipeline stages are sorted based on requisiteStageRefIds`() {
    val basePipeline = Pipeline(name = "pipeline", id = "1", application = appName)

    EXPORTABLE_PIPELINE_SHAPES.forEach { shape ->
      val pipeline = basePipeline.copy(
        _stages = shape.mapIndexed { index, stage ->
          GenericStage(
            name = "whatever",
            type = stage,
            refId = "$index",
            requisiteStageRefIds = if (index == 0) {
              emptyList()
            } else {
              listOf("${index - 1}")
            }
          )
        }
      )
      println(pipeline.shape.joinToString(" -> "))
      expectThat(pipeline.shape).isEqualTo(shape)
    }

    // check a pipeline where the stages are out of order
    basePipeline.copy(
      _stages = listOf(
        GenericStage("bake", "bake", refId = "1", requisiteStageRefIds = listOf("3")),
        GenericStage("deploy", "deploy", refId = "2", requisiteStageRefIds = listOf("1")),
        GenericStage("build", "jenkins", refId = "3", requisiteStageRefIds = emptyList()),
        GenericStage("test", "jenkins", refId = "4", requisiteStageRefIds = listOf("2")),
      )
    ).also { pipeline ->
      expectThat(pipeline.stages.map { it.name }).isEqualTo(listOf("build", "bake", "deploy", "test"))
    }
  }

  @Test
  fun `can export jenkins jobs with tests to verifications`() {
    every {
      front50Cache.pipelinesByApplication(appName)
    } returns listOf(pipelineWithJenkinsTests)

    every {
      jenkinsService.getJobConfig(any(), any())
    } returns jenkinsJobWithTests

    val result = runBlocking {
      subject.exportFromPipelines(appName, includeVerifications = true)
    }

    expectThat(result) {
      isA<PipelineExportResult>().and {
        get { deliveryConfig.environments.first().verifyWith }
          .hasSize(1)
          .first()
          .isA<JenkinsJobVerification>()
          .and {
            get { controller }.isEqualTo(jenkinsStageWithTests.controller)
            get { job }.isEqualTo(jenkinsStageWithTests.job)
            get { staticParameters }.isEqualTo(jenkinsStageWithTests.parameters)
          }
      }
    }
  }

  @Test
  fun `ignores jenkins jobs without tests`() {
    every {
      front50Cache.pipelinesByApplication(appName)
    } returns listOf(pipelineWithJenkinsTests)

    every {
      jenkinsService.getJobConfig(any(), any())
    } returns jenkinsJobWithoutTests

    val result = runBlocking {
      subject.exportFromPipelines(appName, includeVerifications = true)
    }

    expectThat(result) {
      isA<PipelineExportResult>().and {
        get { deliveryConfig.environments.first().verifyWith }
          .isEmpty()
      }
    }
  }

  @Test
  fun `fails to export jenkins jobs with SpEL in parameters and notifies team`() {
    val badPipeline = pipelineWithJenkinsTests.run {
      copy(
        _stages = stages.map {
          if (it is JenkinsStage) {
            it.copy(parameters = mapOf("spelParam" to "\${var}"))
          } else {
            it
          }
        }
      )
    }

    every {
      front50Cache.pipelinesByApplication(appName)
    } returns listOf(badPipeline)

    every {
      jenkinsService.getJobConfig(any(), any())
    } returns jenkinsJobWithTests

    val result = runBlocking {
      subject.exportFromPipelines(appName, includeVerifications = true)
    }

    expectThat(result) {
      isA<PipelineExportResult>().and {
        get { exportSucceeded }.isFalse()
      }
    }

    verify {
      slackService.postChatMessage("team-channel", any(), any())
    }
  }

  @Test
  fun `throw an exception if a target group not found in the load balancers list`() {
    every {
      clouddriverService.loadBalancersForApplication(any(), any())
    } returns emptyList()
    expectThrows<UnsupportedResourceTypeException> {
      subject.exportFromPipelines(appName)
    }
  }

  @Test
  fun `get the right load balancer name`() {
    val result = runBlocking {
      subject.exportFromPipelines(appName)
    }
    expectThat(result) {
      isA<PipelineExportResult>().and {
        get {
          deliveryConfig.environments.first().resources.filterIsInstance<ApplicationLoadBalancerSpec>().all {
            it.moniker.toName() == loadBalancer.loadBalancerName
          }
        }.isTrue()
      }
    }
  }

  @Test
  fun `load balancer with titus`() {
    every {
      front50Cache.pipelinesByApplication(appName)
    } returns listOf(pipelineWithTitus)

    every { titusClusterHandler.exportArtifact(any()) } returns artifact

    val result = runBlocking {
      subject.exportFromPipelines(appName)
    }
    expectThat(result) {
      isA<PipelineExportResult>().and {
        get {
          deliveryConfig.environments.first().resources.filterIsInstance<ApplicationLoadBalancerSpec>().all {
            it.moniker.toName() == loadBalancer.loadBalancerName
          }
        }.isTrue()
      }
    }
  }

  @Test
  fun `do not export shared security group`() {
    val result = runBlocking {
      subject.exportFromPipelines(appName)
    }
    expectThat(result) {
      isA<PipelineExportResult>().and {
        get {
          deliveryConfig.environments.first().resources.filterIsInstance<SecurityGroupSpec>().any {
            it.moniker.toName() == "nf-something"
          }
        }.isFalse()
      }
    }
  }

  @Test
  fun `application with one unsupported pipeline shape producing floating artifacts, resources and constraints`() {
    every {
      front50Cache.pipelinesByApplication(appName)
    } returns listOf(pipelineWithTwoDeployDifferentResources)
    every { titusClusterHandler.exportArtifact(any()) } returns dockerArtifact

    val result = runBlocking {
      subject.exportFromPipelines(appName)
    }
    expectThat(result) {
      isA<PipelineExportResult>().and {
        get { exported }
          .hasSize(0)
      }
        .and {
          get { skipped }
            .hasSize(1)
            .get { keys }
            .first()
            .and {
              get { resources }
                .isA<Set<SubmittedResource<*>>>()
                .hasSize(5)
                .get {
                  this.any {
                    it.kind == titusCluster.kind
                  }
                }.isTrue()
            }
            .and {
              get { constraints }
                .isA<Set<AllowedTimesConstraint>>()
                .hasSize(1)
            }
            .get { artifacts }
            .isEqualTo(mutableSetOf(artifact, dockerArtifact))
        }
    }
  }

  @Test
  fun `application with two pipelines, one supported and one not, return correct both supported and unsupported pipeline`() {
    every {
      front50Cache.pipelinesByApplication(appName)
    } returns listOf(pipelineWithTwoDeployDifferentResources, pipelineWithTitus)
    every { titusClusterHandler.exportArtifact(any()) } returns dockerArtifact

    val result = runBlocking {
      subject.exportFromPipelines(appName)
    }
    expectThat(result) {
      isA<PipelineExportResult>().and {
        get { exported }
          .hasSize(1)
          .get { keys }
          .first()
          .and {
            get { resources }
              .isA<Set<SubmittedResource<*>>>()
              .hasSize(4)
          }
      }
        .and {
          get { skipped }
            .hasSize(1)
            .get { keys }
            .first()
            .and {
              get { resources }
                .isA<Set<SubmittedResource<*>>>()
                .hasSize(5)
            }
        }
    }
  }

  @Test
  fun `application with an unsupported pipeline, with a canary stage`() {
    every {
      front50Cache.pipelinesByApplication(appName)
    } returns listOf(pipelineWithCanary)

    val result = runBlocking {
      subject.exportFromPipelines(appName)
    }
    expectThat(result) {
      isA<PipelineExportResult>()
        .and {
          get { skipped }
            .hasSize(1)
            .get { keys }
            .first()
            .and {
              get { constraints }
                .isA<Set<CanaryConstraint>>()
                .hasSize(1)
            }
        }
    }
  }

  @Test
  fun `application with a pipeline containing two deploy stages with different regions, exports the right regions`() {
    every {
      front50Cache.pipelinesByApplication(appName)
    } returns listOf(pipelineWithTwoDeployDifferentRegions)

    val result = runBlocking {
      subject.exportFromPipelines(appName)
    }


    val exportable = slot<Exportable>()

    verify(exactly = 1) {
      ec2ClusterHandler.export(capture(exportable))
    }

    expectThat(result) {
      isA<PipelineExportResult>().and {
        get {
          skipped.keys.first().resources?.first {
            it.kind == ec2Cluster.kind
          }
        }.isA<SubmittedResource<ClusterSpec>>()
          .get { spec }
          .get {
            locations.regions
              .map { it.name }
              .toSet()
          }.contains(setOf(region1, region2, region3))

      }
    }
    expectThat(exportable).captured.get { regions }.hasSize(3).containsExactlyInAnyOrder(region1, region2, region3)
  }
}
