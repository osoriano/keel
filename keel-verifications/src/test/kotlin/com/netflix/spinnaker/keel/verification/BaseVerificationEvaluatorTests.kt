package com.netflix.spinnaker.keel.verification

import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.PullRequest
import com.netflix.spinnaker.keel.api.ec2.ApplicationLoadBalancerSpec
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.artifacts.repoUrl
import com.netflix.spinnaker.keel.network.NetworkEndpoint
import com.netflix.spinnaker.keel.network.NetworkEndpointProvider
import com.netflix.spinnaker.keel.network.NetworkEndpointType.DNS
import com.netflix.spinnaker.keel.network.NetworkEndpointType.EUREKA_CLUSTER_DNS
import com.netflix.spinnaker.keel.network.NetworkEndpointType.EUREKA_VIP_DNS
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.test.deliveryConfigWithClusterAndLoadBalancer
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_ARTIFACT_VERSION
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_BRANCH_NAME
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_BUILD_NUMBER
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_COMMIT_SHA
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_COMMIT_URL
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_ENV
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_EUREKA_CLUSTER
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_EUREKA_VIP
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_LOAD_BALANCER
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_PR_NUMBER
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_PR_URL
import com.netflix.spinnaker.keel.verification.StandardTestParameter.TEST_REPO_URL
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.endsWith
import strikt.assertions.getValue
import strikt.assertions.isEqualTo

abstract class BaseVerificationEvaluatorTests {
  protected abstract val subject: BaseVerificationEvaluator<*>

  protected val testContext = ArtifactInEnvironmentContext(
    deliveryConfig = deliveryConfigWithClusterAndLoadBalancer(),
    environmentName = "test",
    artifactReference = "fnord",
    version = "1.1"
  )

  protected val publishedArtifact = PublishedArtifact(
    type = DOCKER,
    name = "fnord",
    version = "0.161.0-h61.116f116",
    reference = "my.docker.registry/fnord/fnord_0.161.0-h61.116f116",
    metadata = mapOf("buildNumber" to "61", "commitId" to "116f116"),
    provenance = "https://my.jenkins.master/jobs/fnord-release/60",
    gitMetadata = GitMetadata(
      commit = "116f116",
      branch = "main",
      commitInfo = Commit(
        link = "https://stash/fnord/fnord/commits/116f116",
        sha = "116f116"
      ),
      pullRequest = PullRequest(
        number = "1",
        url = "https://stash/fnord/fnord/pull-requests/1"
      )
    ),
    buildMetadata = BuildMetadata(
      id = 61,
      number = "61",
      status = "BUILDING",
      uid = "just-a-uid-obviously"
    )
  ).normalized()

  protected val keelRepository: KeelRepository = mockk {
    coEvery { getArtifactVersion(any(), any(), any()) } returns publishedArtifact
  }

  protected val eurekaClusterDns = "fnord-test-cluster.cluster.us-east-1.keel.io"
  protected val eurekaVipDns = "fnord-test-cluster.vip.us-east-1.keel.io"
  protected val albDns = "internal-fnord-test-alb-vpc0-1234567890.us-east-1.elb.amazonaws.com"

  protected val endpointProvider: NetworkEndpointProvider = mockk {
    coEvery {
      getNetworkEndpoints(any())
    } answers {
      when (arg<Resource<*>>(0).spec) {
        is TitusClusterSpec -> setOf(
          NetworkEndpoint(
            EUREKA_CLUSTER_DNS, "us-east-1", eurekaClusterDns
          ),
          NetworkEndpoint(
            EUREKA_VIP_DNS, "us-east-1", eurekaVipDns
          ),
        )
        is ApplicationLoadBalancerSpec -> setOf(
          NetworkEndpoint(
            DNS, "us-east-1", albDns
          )
        )
        else -> throw IllegalStateException("this is a bug in the test")
      }
    }
  }

  @Test
  fun `standard test parameters are correctly calculated`() {
    val standardParams = runBlocking { subject.getStandardTestParameters(testContext) }

    expectThat(standardParams) {
        getValue(TEST_ENV.name).isEqualTo(testContext.environmentName)
        getValue(TEST_REPO_URL.name).endsWith(publishedArtifact.repoUrl("#prefix#").removePrefix("#prefix#"))
        getValue(TEST_BUILD_NUMBER.name).isEqualTo(publishedArtifact.buildNumber.toString())
        getValue(TEST_ARTIFACT_VERSION.name).isEqualTo(publishedArtifact.version)
        getValue(TEST_BRANCH_NAME.name).isEqualTo(publishedArtifact.gitMetadata?.branch)
        getValue(TEST_COMMIT_SHA.name).isEqualTo(publishedArtifact.gitMetadata?.commit)
        getValue(TEST_COMMIT_URL.name).isEqualTo(publishedArtifact.gitMetadata?.commitInfo?.link)
        getValue(TEST_PR_NUMBER.name).isEqualTo(publishedArtifact.gitMetadata?.pullRequest?.number.toString())
        getValue(TEST_PR_URL.name).isEqualTo(publishedArtifact.gitMetadata?.pullRequest?.url)
        getValue(TEST_EUREKA_VIP.name).isEqualTo(eurekaVipDns)
        getValue(TEST_EUREKA_CLUSTER.name).isEqualTo(eurekaClusterDns)
        getValue(TEST_LOAD_BALANCER.name).isEqualTo(albDns)
    }
  }
}
