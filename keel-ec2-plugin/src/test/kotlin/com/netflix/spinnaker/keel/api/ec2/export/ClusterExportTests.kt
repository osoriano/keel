package com.netflix.spinnaker.keel.api.ec2.export

import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.Exportable
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.RedBlack
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.SubnetAwareRegionSpec
import com.netflix.spinnaker.keel.api.artifacts.ArtifactOriginFilter
import com.netflix.spinnaker.keel.api.artifacts.BranchFilter
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.ec2.ClusterDependencies
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.CapacitySpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.ServerGroupSpec
import com.netflix.spinnaker.keel.api.ec2.CustomizedMetricSpecification
import com.netflix.spinnaker.keel.api.ec2.EC2ScalingSpec
import com.netflix.spinnaker.keel.api.ec2.EC2_CLOUD_PROVIDER
import com.netflix.spinnaker.keel.api.ec2.EC2_CLUSTER_V1_1
import com.netflix.spinnaker.keel.api.ec2.LaunchConfigurationSpec
import com.netflix.spinnaker.keel.api.ec2.ServerGroup.ActiveServerGroupImage
import com.netflix.spinnaker.keel.api.ec2.ServerGroup.LaunchConfiguration.Companion.defaultIamRoleFor
import com.netflix.spinnaker.keel.api.ec2.TargetTrackingPolicy
import com.netflix.spinnaker.keel.api.ec2.TerminationPolicy
import com.netflix.spinnaker.keel.api.ec2.VirtualMachineImage
import com.netflix.spinnaker.keel.api.ec2.resolve
import com.netflix.spinnaker.keel.api.plugins.Resolver
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.clouddriver.model.ActiveServerGroup
import com.netflix.spinnaker.keel.clouddriver.model.Network
import com.netflix.spinnaker.keel.clouddriver.model.SecurityGroupSummary
import com.netflix.spinnaker.keel.clouddriver.model.Subnet
import com.netflix.spinnaker.keel.diff.DefaultResourceDiffFactory
import com.netflix.spinnaker.keel.ec2.resource.BlockDeviceConfig
import com.netflix.spinnaker.keel.ec2.resource.ClusterHandler
import com.netflix.spinnaker.keel.ec2.resource.toCloudDriverResponse
import com.netflix.spinnaker.keel.exceptions.ArtifactMetadataUnavailableException
import com.netflix.spinnaker.keel.exceptions.TagToReleaseArtifactException
import com.netflix.spinnaker.keel.igor.artifact.ArtifactService
import com.netflix.spinnaker.keel.jenkins.JenkinsService
import com.netflix.spinnaker.keel.orca.ClusterExportHelper
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.orca.OrcaTaskLauncher
import com.netflix.spinnaker.keel.orca.TaskRefResponse
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.retrofit.RETROFIT_NOT_FOUND
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import com.netflix.spinnaker.keel.test.resource
import io.mockk.clearAllMocks
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.containsKey
import strikt.assertions.hasSize
import strikt.assertions.isA
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEmpty
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import java.time.Clock
import java.util.*
import com.netflix.spinnaker.keel.clouddriver.model.Capacity as ClouddriverCapacity
import io.mockk.coEvery as every
import org.springframework.core.env.Environment as SpringEnv

internal class ClusterExportTests {

  val cloudDriverService = mockk<CloudDriverService>()
  val cloudDriverCache = mockk<CloudDriverCache>()
  val orcaService = mockk<OrcaService>()
  val normalizers = emptyList<Resolver<ClusterSpec>>()
  val clock: Clock = Clock.systemUTC()
  val publisher: EventPublisher = mockk(relaxUnitFun = true)
  val springEnv: SpringEnv = mockk(relaxUnitFun = true)
  val repository = mockk<KeelRepository>()
  val taskLauncher = OrcaTaskLauncher(
    orcaService,
    repository,
    publisher,
    springEnv
  )
  val clusterExportHelper = mockk<ClusterExportHelper>(relaxed = true)
  val blockDeviceConfig = mockk<BlockDeviceConfig>()
  val artifactService = mockk<ArtifactService>()
  val jenkinsService = mockk<JenkinsService>()

  val vpcWest = Network(EC2_CLOUD_PROVIDER, "vpc-1452353", "vpc0", "test", "us-west-2")
  val vpcEast = Network(EC2_CLOUD_PROVIDER, "vpc-4342589", "vpc0", "test", "us-east-1")
  val sg1West = SecurityGroupSummary("keel", "sg-325234532", "vpc-1")
  val sg2West = SecurityGroupSummary("keel-elb", "sg-235425234", "vpc-1")
  val sg1East = SecurityGroupSummary("keel", "sg-279585936", "vpc-1")
  val sg2East = SecurityGroupSummary("keel-elb", "sg-610264122", "vpc-1")
  val subnet1West =
    Subnet("subnet-1", vpcWest.id, vpcWest.account, vpcWest.region, "${vpcWest.region}a", "internal (vpc0)")
  val subnet2West =
    Subnet("subnet-2", vpcWest.id, vpcWest.account, vpcWest.region, "${vpcWest.region}b", "internal (vpc0)")
  val subnet3West =
    Subnet("subnet-3", vpcWest.id, vpcWest.account, vpcWest.region, "${vpcWest.region}c", "internal (vpc0)")
  val subnet1East =
    Subnet("subnet-1", vpcEast.id, vpcEast.account, vpcEast.region, "${vpcEast.region}c", "internal (vpc0)")
  val subnet2East =
    Subnet("subnet-2", vpcEast.id, vpcEast.account, vpcEast.region, "${vpcEast.region}d", "internal (vpc0)")
  val subnet3East =
    Subnet("subnet-3", vpcEast.id, vpcEast.account, vpcEast.region, "${vpcEast.region}e", "internal (vpc0)")

  val targetTrackingPolicyName = "keel-test-target-tracking-policy"

  val spec = ClusterSpec(
    moniker = Moniker(app = "keel", stack = "test"),
    locations = SubnetAwareLocations(
      account = vpcWest.account,
      vpc = "vpc0",
      subnet = subnet1West.purpose!!,
      regions = listOf(vpcWest, vpcEast).map { subnet ->
        SubnetAwareRegionSpec(
          name = subnet.region,
          availabilityZones = listOf("a", "b", "c").map { "${subnet.region}$it" }.toSet()
        )
      }.toSet()
    ),
    deployWith = RedBlack(),
    _defaults = ServerGroupSpec(
      launchConfiguration = LaunchConfigurationSpec(
        image = VirtualMachineImage(
          id = "ami-123543254134",
          appVersion = "keel-0.287.0-h208.fe2e8a1",
          baseImageName = "bionicbase-x86_64-202101262358-ebs"
        ),
        instanceType = "r4.8xlarge",
        ebsOptimized = false,
        iamRole = defaultIamRoleFor("keel"),
        keyPair = "nf-keypair-test-fake",
        instanceMonitoring = false
      ),
      capacity = CapacitySpec(min = 1, max = 6),
      scaling = EC2ScalingSpec(
        targetTrackingPolicies = setOf(
          TargetTrackingPolicy(
            name = targetTrackingPolicyName,
            targetValue = 560.0,
            disableScaleIn = true,
            customMetricSpec = CustomizedMetricSpecification(
              name = "RPS per instance",
              namespace = "SPIN/ACH",
              statistic = "Average"
            )
          )
        )
      ),
      dependencies = ClusterDependencies(
        loadBalancerNames = setOf("keel-test-frontend"),
        securityGroupNames = setOf(sg1West.name, sg2West.name)
      )
    )
  )

  val image = ActiveServerGroupImage(
    imageId = "ami-123543254134",
    name = "keel-0.287.0-h208.fe2e8a1-x86_64-20200413213533-bionic-classic-hvm-sriov-ebs",
    description = "name=keel, arch=x86_64, ancestor_name=bionic-classicbase-x86_64-202002251430-ebs, ancestor_id=ami-0000, ancestor_version=nflx-base-5.308.0-h1044.b4b3f78",
    imageLocation = "1111/keel-0.287.0-h208.fe2e8a1-x86_64-20200413213533-bionic-classic-hvm-sriov-ebs",
    appVersion = null,
    baseImageName = null
  )

  val serverGroups = spec.resolve()
  val serverGroupEast = serverGroups.first { it.location.region == "us-east-1" }.copy(image = image)
  val serverGroupWest = serverGroups.first { it.location.region == "us-west-2" }.copy(image = image)

  val resource = resource(
    kind = EC2_CLUSTER_V1_1.kind,
    spec = spec
  )

  val activeServerGroupResponseEast = serverGroupEast.toCloudDriverResponse(
    vpcEast,
    listOf(subnet1East, subnet2East, subnet3East),
    listOf(sg1East, sg2East),
    image
  )
  val activeServerGroupResponseWest = serverGroupWest.toCloudDriverResponse(
    vpcWest,
    listOf(subnet1West, subnet2West, subnet3West),
    listOf(sg1West, sg2West),
    image
  )

  val exportable = Exportable(
    cloudProvider = "aws",
    account = spec.locations.account,
    user = "fzlem@netflix.com",
    moniker = spec.moniker,
    regions = spec.locations.regions.map { it.name }.toSet(),
    kind = EC2_CLUSTER_V1_1.kind
  )

  val mapper = configuredTestObjectMapper()
  val stepScalingServerGroupJson: String = checkNotNull(javaClass.getResource("/cluster-with-step-scaling-policies.json")).readText()
  val stepScalingServerGroup: ActiveServerGroup = mapper.readValue<ActiveServerGroup>(stepScalingServerGroupJson)

  val subject = ClusterHandler(
    cloudDriverService,
    cloudDriverCache,
    orcaService,
    taskLauncher,
    clock,
    publisher,
    normalizers,
    clusterExportHelper,
    blockDeviceConfig,
    artifactService,
    DefaultResourceDiffFactory()
  )

  @BeforeEach
  fun setup() {
    with(cloudDriverCache) {
      every { defaultKeyPairForAccount("test") } returns "nf-keypair-test-{{region}}"

	    every { networkBy(vpcWest.id) } returns vpcWest
	    every { subnetBy(subnet1West.id) } returns subnet1West
	    every { subnetBy(subnet2West.id) } returns subnet2West
	    every { subnetBy(subnet3West.id) } returns subnet3West
	    every { subnetBy(vpcWest.account, vpcWest.region, subnet1West.purpose!!) } returns subnet1West
	    every { securityGroupById(vpcWest.account, vpcWest.region, sg1West.id, any()) } returns sg1West
	    every { securityGroupById(vpcWest.account, vpcWest.region, sg2West.id, any()) } returns sg2West
	    every { securityGroupByName(vpcWest.account, vpcWest.region, sg1West.name, any()) } returns sg1West
	    every { securityGroupByName(vpcWest.account, vpcWest.region, sg2West.name, any()) } returns sg2West
	    every { availabilityZonesBy(vpcWest.account, vpcWest.id, subnet1West.purpose!!, vpcWest.region) } returns
	      setOf(subnet1West.availabilityZone)

	    every { networkBy(vpcEast.id) } returns vpcEast
	    every { subnetBy(subnet1East.id) } returns subnet1East
	    every { subnetBy(subnet2East.id) } returns subnet2East
	    every { subnetBy(subnet3East.id) } returns subnet3East
	    every { subnetBy(vpcEast.account, vpcEast.region, subnet1East.purpose!!) } returns subnet1East
	    every { securityGroupById(vpcEast.account, vpcEast.region, sg1East.id, any()) } returns sg1East
	    every { securityGroupById(vpcEast.account, vpcEast.region, sg2East.id, any()) } returns sg2East
	    every { securityGroupByName(vpcEast.account, vpcEast.region, sg1East.name, any()) } returns sg1East
	    every { securityGroupByName(vpcEast.account, vpcEast.region, sg2East.name, any()) } returns sg2East
	    every { availabilityZonesBy(vpcEast.account, vpcEast.id, subnet1East.purpose!!, vpcEast.region) } returns
	      setOf(subnet1East.availabilityZone)
    }

    every {
      orcaService.orchestrate(
        resource.serviceAccount,
        any(),
        any()
      )
    } returns TaskRefResponse("/tasks/${UUID.randomUUID()}")
    every { repository.environmentFor(any()) } returns Environment("test")
    every {
      clusterExportHelper.discoverDeploymentStrategy("aws", "test", "keel", any())
    } returns RedBlack()

    every {
      springEnv.getProperty("keel.notifications.slack", Boolean::class.java, true)
    } returns false
  }

  @AfterEach
  fun cleanup() {
    clearAllMocks()
  }

  @Test
  fun `converting step scaling policies`() {
    val serverGroup = subject.convertServerGroup(stepScalingServerGroup)
    expectThat(serverGroup.scaling.targetTrackingPolicies).isEmpty()
    expectThat(serverGroup.scaling.stepScalingPolicies).hasSize(1)
  }

  @Nested
  @DisplayName("exporting an artifact from a cluster")
  inner class ExportArtifact {

    @BeforeEach
    fun setup() {
      every { cloudDriverService.activeServerGroup(any(), "us-east-1") } returns activeServerGroupResponseEast
      every {
        artifactService.getArtifact("keel", any(), DEBIAN)
      } returns PublishedArtifact(
        name = "keel",
        reference = "keel",
        type = DEBIAN,
        version = "0.0.1",
        metadata = mapOf(
          "branch" to "main",
          "commitId" to "f80cfcfdec37df59604b2ef93dfb29bade340791",
          "buildNumber" to "41"
        ),
        provenance = "https://blablabla.net/job/users-my-app-build/"
      )
    }

    @Test
    fun `deb is exported correctly and includes from spec with branch`() {
      val artifact = runBlocking {
        subject.exportArtifact(exportable.copy(regions = setOf("us-east-1")))
      }

      expectThat(artifact) {
        get { name }.isEqualTo("keel")
        isA<DebianArtifact>()
          .and {
            get { vmOptions }.isEqualTo(
              VirtualMachineOptions(
                regions = setOf("us-east-1"),
                baseOs = "bionic-classic"
              )
            )
          }
          .and {
            get { from }.isEqualTo(ArtifactOriginFilter(branch = BranchFilter(name = "main")))
          }
      }
    }
  }

  @Nested
  @DisplayName("with invalid branch metadata - branch name is set to be commit hash")
  inner class InvalidMetadata {
    @BeforeEach
    fun setup() {
      every { cloudDriverService.activeServerGroup(any(), "us-east-1") } returns activeServerGroupResponseEast
      every {
        artifactService.getArtifact("keel", any(), DEBIAN)
      } returns PublishedArtifact(
        name = "keel",
        reference = "keel",
        type = DEBIAN,
        version = "0.0.1",
        metadata = mapOf("branch" to "main"),
        provenance = "https://blablabla.net/job/users-my-app-build/",
        // the branch has a commit hash instead of a branch name, which is not allowed in the export
        gitMetadata = GitMetadata(commit = "b84af827736", branch = "b84af827736")
      )
    }

    @Test
    fun `artifact with tag-to-release is exported and the exception is captured`() {
      val artifact = runBlocking {
        subject.exportArtifact(exportable.copy(regions = setOf("us-east-1")))
      }
      expectThat(artifact) {
        get { name }.isEqualTo("keel")
        isA<DebianArtifact>()
          .and {
            get { exportWarning }.isA<TagToReleaseArtifactException>()
          }
      }

    }
  }

  @Nested
  @DisplayName("with artifact that is not on rocket")
  inner class NotOnRocket {

    @BeforeEach
    fun setup() {
      every { cloudDriverService.activeServerGroup(any(), "us-east-1") } returns activeServerGroupResponseEast
      every {
        artifactService.getArtifact("keel", any(), DEBIAN)
      } throws RETROFIT_NOT_FOUND
    }

    @Test
    fun `artifact is not exported and exception is thrown`() {
      expectThrows<ArtifactMetadataUnavailableException> {
        subject.exportArtifact(exportable.copy(regions = setOf("us-east-1")))
      }
    }
  }

  @Nested
  @DisplayName("basic export behavior")
  inner class BasicExport {
    @BeforeEach
    fun setup() {
      every { cloudDriverService.activeServerGroup(any(), "us-east-1") } returns activeServerGroupResponseEast
    }

    @Test
    fun `deployment strategy defaults are omitted`() {
      val cluster = runBlocking {
        subject.export(exportable.copy(regions = setOf("us-east-1")))
      }

      expectThat(cluster.deployWith) {
        isA<RedBlack>().and {
          get { maxServerGroups }.isNull()
          get { delayBeforeDisable }.isNull()
          get { resizePreviousToZero }.isNull()
          get { delayBeforeScaleDown }.isNull()
        }
      }
    }
  }

  @Nested
  @DisplayName("exporting same clusters different regions")
  inner class ExportDifferentRegions {
    @BeforeEach
    fun setup() {
      every { cloudDriverService.activeServerGroup(any(), "us-east-1") } returns activeServerGroupResponseEast
      every { cloudDriverService.activeServerGroup(any(), "us-west-2") } returns activeServerGroupResponseWest
    }

    @Test
    fun `no overrides`() {
      val cluster = runBlocking {
        subject.export(exportable)
      }
      expectThat(cluster) {
        get { locations.regions }.hasSize(2)
        get { overrides }.hasSize(0)
        get { defaults.scaling }.isA<EC2ScalingSpec>().get { targetTrackingPolicies }.hasSize(1)
        get { defaults.health }.isNull()
        get { deployWith }.isA<RedBlack>()
        get { artifactReference }.isNotNull()
      }
    }
  }

  @Nested
  @DisplayName("exporting clusters with capacity difference between regions")
  inner class DifferentCapacity {
    @BeforeEach
    fun setup() {
      every { cloudDriverService.activeServerGroup(any(), "us-east-1") } returns activeServerGroupResponseEast
      every { cloudDriverService.activeServerGroup(any(), "us-west-2") } returns activeServerGroupResponseWest
        .withDifferentSize()
    }

    @Test
    fun `override only in capacity`() {
      val cluster = runBlocking {
        subject.export(exportable)
      }
      expectThat(cluster) {
        get { locations.regions }.hasSize(2)
        get { overrides }.hasSize(1)
        get { overrides }.get { "us-east-1" }.get { "capacity" }.isNotEmpty()
        get { defaults.scaling }.isA<EC2ScalingSpec>().get { targetTrackingPolicies }.hasSize(1)
        get { spec.defaults.health }.isNull()
      }
    }
  }

  @Nested
  @DisplayName("exporting clusters with capacity difference between regions")
  inner class DefaultCapacity {
    @BeforeEach
    fun setup() {
      every { cloudDriverService.activeServerGroup(any(), "us-east-1") } returns activeServerGroupResponseEast.copy(
        capacity = ClouddriverCapacity(1, 1, 1)
      )
      every { cloudDriverService.activeServerGroup(any(), "us-west-2") } returns activeServerGroupResponseWest.copy(
        capacity = ClouddriverCapacity(3, 3, 3)
      )
    }

    @Test
    fun `override only in capacity`() {
      val cluster = runBlocking {
        subject.export(exportable)
      }
      with(cluster) {
        expect {
          that(overrides).hasSize(1)
          that(overrides).containsKey("us-east-1")
          val override = overrides["us-east-1"]
          that(override).isNotNull().get { capacity }.isEqualTo(CapacitySpec(1,1,null))
        }
      }
    }
  }


  @Nested
  @DisplayName("exporting clusters that are significantly different between regions")
  inner class DifferenceBetweenRegions {
    @BeforeEach
    fun setup() {
      every { cloudDriverService.activeServerGroup(any(), "us-east-1") } returns activeServerGroupResponseEast
      every { cloudDriverService.activeServerGroup(any(), "us-west-2") } returns activeServerGroupResponseWest
        .withNonDefaultHealthProps()
        .withNonDefaultLaunchConfigProps()
    }

    @Test
    fun `export omits properties with default values from complex fields`() {
      val exported = runBlocking {
        subject.export(exportable)
      }
      expect {
        that(exported.locations.subnet).isNull()
        that(exported.defaults.health).isNotNull()

        that(exported.defaults.health!!) {
          get { cooldown }.isNull()
          get { warmup }.isNull()
          get { healthCheckType }.isNull()
          get { enabledMetrics }.isNull()
          get { terminationPolicies }.isEqualTo(setOf(TerminationPolicy.NewestInstance))
        }
        that(exported.defaults.launchConfiguration).isNotNull()
        that(exported.defaults.launchConfiguration!!) {
          get { ebsOptimized }.isNull()
          get { instanceMonitoring }.isNull()
          get { ramdiskId }.isNull()
          get { instanceType }.isNotNull()
          get { iamRole }.isNotNull()
          get { keyPair }.isNotNull()
        }
      }
    }
  }

  private suspend fun CloudDriverService.activeServerGroup(user: String, region: String) = activeServerGroup(
    user = user,
    app = spec.moniker.app,
    account = spec.locations.account,
    cluster = spec.moniker.toString(),
    region = region,
    cloudProvider = EC2_CLOUD_PROVIDER
  )
}

private fun ActiveServerGroup.withNonDefaultHealthProps(): ActiveServerGroup =
  copy(
    asg = asg.copy(
      terminationPolicies = setOf(TerminationPolicy.NewestInstance.name)
    )
  )

private fun ActiveServerGroup.withNonDefaultLaunchConfigProps(): ActiveServerGroup =
  copy(
    launchConfig = launchConfig?.copy(
      iamInstanceProfile = "NotTheDefaultInstanceProfile",
      keyName = "not-the-default-key"
    )
  )

private fun ActiveServerGroup.withDifferentSize(): ActiveServerGroup =
  copy(
    capacity = ClouddriverCapacity(min = 1, max = 10, desired = 5)
  )
