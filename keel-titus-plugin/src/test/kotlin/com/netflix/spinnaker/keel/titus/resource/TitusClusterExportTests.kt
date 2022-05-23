package com.netflix.spinnaker.keel.titus.resource

import com.netflix.spinnaker.keel.api.ArtifactBridge
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.Exportable
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.RedBlack
import com.netflix.spinnaker.keel.api.SimpleLocations
import com.netflix.spinnaker.keel.api.SimpleRegionSpec
import com.netflix.spinnaker.keel.api.artifacts.ArtifactMetadata
import com.netflix.spinnaker.keel.api.artifacts.ArtifactOriginFilter
import com.netflix.spinnaker.keel.api.artifacts.BranchFilter
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.DockerImage
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.BRANCH_JOB_COMMIT_BY_JOB
import com.netflix.spinnaker.keel.api.ec2.ClusterDependencies
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.CapacitySpec
import com.netflix.spinnaker.keel.api.plugins.Resolver
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.api.titus.ResourcesSpec
import com.netflix.spinnaker.keel.api.titus.TITUS_CLOUD_PROVIDER
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.api.titus.TitusScalingSpec
import com.netflix.spinnaker.keel.api.titus.TitusServerGroup.NetworkMode.HighScale
import com.netflix.spinnaker.keel.api.titus.TitusServerGroupSpec
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.clouddriver.model.*
import com.netflix.spinnaker.keel.diff.DefaultResourceDiffFactory
import com.netflix.spinnaker.keel.docker.DigestProvider
import com.netflix.spinnaker.keel.docker.ReferenceProvider
import com.netflix.spinnaker.keel.orca.ClusterExportHelper
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.orca.OrcaTaskLauncher
import com.netflix.spinnaker.keel.orca.TaskRefResponse
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.test.resource
import com.netflix.spinnaker.keel.titus.DefaultContainerAttributes
import com.netflix.spinnaker.keel.titus.NETFLIX_CONTAINER_ENV_VARS
import com.netflix.spinnaker.keel.titus.TitusClusterHandler
import com.netflix.spinnaker.keel.titus.registry.TitusRegistryService
import com.netflix.spinnaker.keel.titus.resolve
import io.mockk.clearAllMocks
import io.mockk.confirmVerified
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.core.env.ConfigurableEnvironment
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.all
import strikt.assertions.containsExactly
import strikt.assertions.containsKey
import strikt.assertions.hasSize
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEmpty
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import java.time.Clock
import java.util.UUID
import io.mockk.coEvery as every

internal class TitusClusterExportTests {
  val titusAccount = "titustest"
  val awsAccount = "test"

  val titusAccountCredential = Credential(
    name = titusAccount,
    type = "titus",
    environment = "testenv",
    attributes = mutableMapOf("awsAccount" to awsAccount, "registry" to "testregistry")
  )

  val cloudDriverService = mockk<CloudDriverService>()
  val cloudDriverCache = mockk<CloudDriverCache> {
    every { credentialBy(titusAccount) } returns titusAccountCredential
  }
  val orcaService = mockk<OrcaService>()
  val resolvers = emptyList<Resolver<TitusClusterSpec>>()
  val repository = mockk<KeelRepository>()
  val publisher: EventPublisher = mockk(relaxUnitFun = true)
  val springEnv: ConfigurableEnvironment = mockk(relaxUnitFun = true)
  val artifactBridge: ArtifactBridge = mockk()
  val titusRegistryService = mockk<TitusRegistryService>()

  val taskLauncher = OrcaTaskLauncher(
    orcaService,
    repository,
    publisher,
    springEnv
  )
  val clock: Clock = Clock.systemUTC()
  val clusterExportHelper = mockk<ClusterExportHelper>(relaxed = true)

  val sg1West = SecurityGroupSummary("keel", "sg-325234532", "vpc-1")
  val sg2West = SecurityGroupSummary("keel-elb", "sg-235425234", "vpc-1")
  val sg1East = SecurityGroupSummary("keel", "sg-279585936", "vpc-1")
  val sg2East = SecurityGroupSummary("keel-elb", "sg-610264122", "vpc-1")

  val container = DigestProvider(
    organization = "spinnaker",
    image = "keel",
    digest = "sha:1111"
  )

  val spec = TitusClusterSpec(
    moniker = Moniker(app = "keel", stack = "test"),
    locations = SimpleLocations(
      account = titusAccount,
      regions = setOf(SimpleRegionSpec("us-east-1"), SimpleRegionSpec("us-west-2"))
    ),
    _defaults = TitusServerGroupSpec(
      capacity = CapacitySpec(1, 6, 4),
      dependencies = ClusterDependencies(
        loadBalancerNames = setOf("keel-test-frontend"),
        securityGroupNames = setOf(sg1West.name)
      ),
      networkMode = HighScale
    ),
    container = container
  )

  val serverGroups = spec.resolve()
  val serverGroupEast = serverGroups.first { it.location.region == "us-east-1" }
  val serverGroupWest = serverGroups.first { it.location.region == "us-west-2" }

  val activeServerGroupResponseEast = serverGroupEast.toClouddriverResponse(listOf(sg1East, sg2East), awsAccount)
  val activeServerGroupResponseWest = serverGroupWest.toClouddriverResponse(listOf(sg1West, sg2West), awsAccount)

  val resource = resource(
    kind = TITUS_CLUSTER_V1.kind,
    spec = spec
  )

  val exportable = Exportable(
    cloudProvider = "titus",
    account = spec.locations.account,
    user = "fzlem@netflix.com",
    moniker = spec.moniker,
    regions = spec.locations.regions.map { it.name }.toSet(),
    kind = TITUS_CLUSTER_V1.kind
  )

  val image = DockerImage(
    account = "testregistry",
    repository = "emburns/spin-titus-demo",
    tag = "1",
    digest = "sha:1111",
    commitId = "commit123", buildNumber = "10"
  )

  val images = listOf(
    image,
    image.copy(tag = "2", digest = "sha:2222")
  )

  val branchJobShaImages = listOf(
    image.copy(tag = "master-h10.62bbbd6"),
    image.copy(tag = "master-h11.4e26fbd", digest = "sha:2222",  commitId = "commit123", buildNumber = "10")
  )

  val weirdImages = listOf(
    image.copy(tag = "blahblah")
  )

  val imageWithLatestTag = listOf(
    image.copy(tag = "latest")
  )

  val imagesWithArtifactInfo = branchJobShaImages
    .map { it.copy(branch = "main", commitId = "commit123", buildNumber = "10") }

  val subject = TitusClusterHandler(
    cloudDriverService,
    cloudDriverCache,
    orcaService,
    clock,
    taskLauncher,
    publisher,
    resolvers,
    clusterExportHelper,
    DefaultResourceDiffFactory(),
    titusRegistryService,
    artifactBridge,
    DefaultContainerAttributes()
  )

  @BeforeEach
  fun setup() {
    with(cloudDriverCache) {
      every { securityGroupById(awsAccount, "us-west-2", sg1West.id, any()) } returns sg1West
      every { securityGroupById(awsAccount, "us-west-2", sg2West.id, any()) } returns sg2West
      every { securityGroupByName(awsAccount, "us-west-2", sg1West.name, any()) } returns sg1West
      every { securityGroupByName(awsAccount, "us-west-2", sg2West.name, any()) } returns sg2West

      every { securityGroupById(awsAccount, "us-east-1", sg1East.id, any()) } returns sg1East
      every { securityGroupById(awsAccount, "us-east-1", sg2East.id, any()) } returns sg2East
      every { securityGroupByName(awsAccount, "us-east-1", sg1East.name, any()) } returns sg1East
      every { securityGroupByName(awsAccount, "us-east-1", sg2East.name, any()) } returns sg2East

      every { cloudDriverCache.credentialBy(titusAccount) } returns titusAccountCredential
      every { cloudDriverCache.getRegistryForTitusAccount(any()) } returns "testregistry"

      every { networkBy(any(), any(), any()) } returns Network("aws", "vpc-123", "vpc0", "test", "us-east-1")

    }
    every { orcaService.orchestrate(resource.serviceAccount, any(), any()) } returns TaskRefResponse("/tasks/${UUID.randomUUID()}")
    every { repository.environmentFor(any()) } returns Environment("test")
    every {
      clusterExportHelper.discoverDeploymentStrategy("titus", "titustest", "keel", any())
    } returns RedBlack()

    every {
      springEnv.getProperty("keel.notifications.slack", Boolean::class.java, true)
    } returns false
  }

  @AfterEach
  fun cleanup() {
    confirmVerified(orcaService)
    clearAllMocks()
  }

  @Nested
  inner class ScalingPolicies {
    @BeforeEach
    fun setup() {
      every { cloudDriverService.titusActiveServerGroup(any(), "us-east-1") } returns activeServerGroupResponseEast.copy(
        scalingPolicies = listOf(
          TitusScaling(
            id = "123-445",
            policy = TitusScaling.Policy.TargetPolicy(
              targetPolicyDescriptor = TargetPolicyDescriptor(
                targetValue = 50.0,
                scaleOutCooldownSec = 300,
                scaleInCooldownSec = 300,
                disableScaleIn = true,
                customizedMetricSpecification = CustomizedMetricSpecificationModel(
                  metricName = "AverageCPUUtilization",
                  namespace = "bunbun",
                  statistic = "Average",
                  dimensions = listOf(MetricDimensionModel(
                    name = "AutoScalingGroupName",
                    value = "emburnstest-bunbun-v008"
                  ))
                )
              )
            )
          )
        )
      )
      every { cloudDriverService.titusActiveServerGroup(any(), "us-west-2") } returns activeServerGroupResponseWest.copy(
        scalingPolicies = listOf(
          TitusScaling(
            id = "123-445",
            policy = TitusScaling.Policy.TargetPolicy(
              targetPolicyDescriptor = TargetPolicyDescriptor(
                targetValue = 50.0,
                scaleOutCooldownSec = 300,
                scaleInCooldownSec = 300,
                disableScaleIn = true,
                customizedMetricSpecification = CustomizedMetricSpecificationModel(
                  metricName = "AverageCPUUtilization",
                  namespace = "bunbun",
                  statistic = "Average",
                  dimensions = listOf(MetricDimensionModel(
                    name = "AutoScalingGroupName",
                    value = "emburnstest-bunbun-v008"
                  ))
                )
              )
            )
          )
        )
      )
      every { cloudDriverService.findDockerImages("testregistry", (spec.container as DigestProvider).repository()) } returns images
      every { cloudDriverCache.credentialBy(titusAccount) } returns titusAccountCredential
    }

    fun `has scaling policies defined`() {
      val cluster = runBlocking {
        subject.export(exportable)
      }
      expectThat(cluster.defaults.scaling).isA<TitusScalingSpec>().get { targetTrackingPolicies }.isNotEmpty()
    }
  }

  @Nested
  inner class WithoutOverrides {

    @BeforeEach
    fun setup() {
      every { cloudDriverService.titusActiveServerGroup(any(), "us-east-1") } returns activeServerGroupResponseEast
      every { cloudDriverService.titusActiveServerGroup(any(), "us-west-2") } returns activeServerGroupResponseWest
      every { cloudDriverService.findDockerImages("testregistry", (spec.container as DigestProvider).repository()) } returns images
      every { cloudDriverCache.credentialBy(titusAccount) } returns titusAccountCredential
    }

    @Test
    fun `exported titus cluster spec`() {
      val cluster = runBlocking {
        subject.export(exportable)
      }
      with(cluster) {
        expect {
          that(locations.regions).hasSize(2)
          that(overrides.values).all {
            // the only overrides added by default are Netflix env vars
            get { env!!.keys }.containsExactly(*NETFLIX_CONTAINER_ENV_VARS)
          }
          that(defaults.constraints).isNull()
          that(defaults.entryPoint).isNull()
          that(defaults.migrationPolicy).isNull()
          that(defaults.resources).isNull()
          that(defaults.iamProfile).isNull()
          that(defaults.capacityGroup).isNull()
          that(defaults.containerAttributes).isNull()
          that(defaults.tags).isNull()
          that(defaults.networkMode) isEqualTo spec.defaults.networkMode
          that(container).isA<ReferenceProvider>()
        }
      }
    }

    @Test
    fun `tags are just increasing numbers - tag strategy is chosen as INCREASING_TAG`() {
      every { titusRegistryService.findImages(any(), any(), any(), any(), any()) } returns images

      every {
        artifactBridge.getArtifactMetadata(any(), any(), any())
      } returns ArtifactMetadata(BuildMetadata(id = 10), GitMetadata(commit = " commit"))

      val artifact = runBlocking {
        subject.exportArtifact(exportable)
      }
      expectThat(artifact)
        .isA<DockerArtifact>()
        .get { tagVersionStrategy }.isEqualTo(TagVersionStrategy.INCREASING_TAG)
    }

    @Test
    fun `tags are branch-job sha - tag strategy is chosen as BRANCH_JOB_COMMIT_BY_JOB`() {
      every {
        titusRegistryService.findImages(any(), any(), any(), any(), any())
      } returns branchJobShaImages

      every {
        artifactBridge.getArtifactMetadata(any(), any(), any())
      } returns ArtifactMetadata(BuildMetadata(id = 10), GitMetadata(commit = " commit"))

      val artifact = runBlocking {
        subject.exportArtifact(exportable)
      }
      expectThat(artifact)
        .isA<DockerArtifact>()
        .get { tagVersionStrategy }.isEqualTo(BRANCH_JOB_COMMIT_BY_JOB)
    }

    @Test
    fun `images have branch information - artifact includes from spec with corresponding branch`() {
      every {
        titusRegistryService.findImages(any(), any(), any(), any(), any())
      } returns imagesWithArtifactInfo

      every {
        artifactBridge.getArtifactMetadata(any(), any(), any())
      } returns ArtifactMetadata(BuildMetadata(id = 10), GitMetadata(commit = " commit"))

      val artifact = runBlocking {
        subject.exportArtifact(exportable)
      }
      expectThat(artifact)
        .isA<DockerArtifact>()
        .get { from }.isEqualTo(ArtifactOriginFilter(branch = BranchFilter(name = "main")))
    }
  }

  @Nested
  inner class WithOverrides {
    @BeforeEach
    fun setup() {
      every { cloudDriverService.titusActiveServerGroup(any(), "us-east-1") } returns
        activeServerGroupResponseEast
          .withDifferentEnv()
          .withDifferentEntryPoint()
          .withDifferentResources()
      every { cloudDriverService.titusActiveServerGroup(any(), "us-west-2") } returns
        activeServerGroupResponseWest
          .withDoubleCapacity()

      every {  titusRegistryService.findImages("testregistry", container.repository()) } returns images
      every { cloudDriverCache.credentialBy(titusAccount) } returns titusAccountCredential
    }

    @Test
    fun `we do not omit the default capacity`() {
      every { cloudDriverService.titusActiveServerGroup(any(), "us-east-1") } returns
        activeServerGroupResponseEast.copy(capacity = Capacity(1, 1, 1))

      every { cloudDriverService.titusActiveServerGroup(any(), "us-west-2") } returns
        activeServerGroupResponseWest.copy(capacity = Capacity(3, 3, 3))

      val cluster = runBlocking {
        subject.export(exportable)
      }
      with(cluster) {
        expect {
          that(overrides).hasSize(1)
          that(overrides).containsKey("us-east-1")
          val override = overrides["us-east-1"]
          that(override).isNotNull().get { capacity }.isEqualTo(CapacitySpec(1,1,1))

        }
      }
    }

    @Test
    fun `has overrides matching differences in the server groups`() {
      val cluster = runBlocking {
        subject.export(exportable)
      }
      with(cluster) {
        expect {
          that(overrides).hasSize(1)
          that(overrides).containsKey("us-east-1")
          val override = overrides["us-east-1"]
          that(override).isNotNull().get { entryPoint }.isNotNull()
          that(override).isNotNull().get { capacity }.isNotNull()
          that(override).isNotNull().get { env }.isNotNull()
          that(override).isNotNull().get { resources }.isEqualTo(
            ResourcesSpec(
              cpu = 4,
              disk = 81920,
              gpu = 0,
              memory = 16384,
              networkMbps = 700
            )
          )

          that(locations.regions).hasSize(2)
        }
      }
    }

    @Test
    fun `has default values in overrides omitted`() {
      val cluster = runBlocking {
        subject.export(exportable)
      }
      with(cluster) {
        expectThat(overrides).containsKey("us-east-1")
        expectThat(overrides["us-east-1"]).isNotNull()
        val override = overrides["us-east-1"]!!
        expectThat(override) {
          get { constraints }.isNull()
          get { migrationPolicy }.isNull()
          get { iamProfile }.isNull()
          get { capacityGroup }.isNull()
          get { containerAttributes }.isNull()
          get { tags }.isNull()
        }
      }
    }
  }

  private suspend fun CloudDriverService.titusActiveServerGroup(user: String, region: String) = titusActiveServerGroup(
    user = user,
    app = spec.moniker.app,
    account = spec.locations.account,
    cluster = spec.moniker.toString(),
    region = region,
    cloudProvider = TITUS_CLOUD_PROVIDER
  )

  private fun TitusActiveServerGroup.withDoubleCapacity(): TitusActiveServerGroup =
    copy(
      capacity = Capacity(
        min = capacity.min * 2,
        max = capacity.max * 2,
        desired = capacity.desired?.let { it * 2 }
      )
    )

  private fun TitusActiveServerGroup.withDifferentEnv(): TitusActiveServerGroup =
    copy(env = mapOf("foo" to "bar"))

  private fun TitusActiveServerGroup.withDifferentEntryPoint(): TitusActiveServerGroup =
    copy(entryPoint = "/bin/blah")

  private fun TitusActiveServerGroup.withDifferentResources(): TitusActiveServerGroup =
    copy(
      resources = Resources(
        cpu = 4,
        disk = 81920,
        gpu = 0,
        memory = 16384,
        networkMbps = 700
      )
    )
}
