package com.netflix.spinnaker.keel.verification

import com.netflix.spinnaker.config.GitLinkConfig
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.ComputeResourceSpec
import com.netflix.spinnaker.keel.api.DependencyType.LOAD_BALANCER
import com.netflix.spinnaker.keel.api.Dependent
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.Verification
import com.netflix.spinnaker.keel.api.plugins.VerificationEvaluator
import com.netflix.spinnaker.keel.artifacts.repoUrl
import com.netflix.spinnaker.keel.network.NetworkEndpoint
import com.netflix.spinnaker.keel.network.NetworkEndpointProvider
import com.netflix.spinnaker.keel.network.NetworkEndpointType.EUREKA_CLUSTER_DNS
import com.netflix.spinnaker.keel.network.NetworkEndpointType.EUREKA_VIP_DNS
import com.netflix.spinnaker.keel.persistence.KeelRepository
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
import org.slf4j.LoggerFactory

/**
 * A base class for all concrete [VerificationEvaluator] implementations.
 */
abstract class BaseVerificationEvaluator<V: Verification>(
  protected open val keelRepository: KeelRepository,
  protected open val gitLinkConfig: GitLinkConfig,
  protected open val networkEndpointProvider: NetworkEndpointProvider
) : VerificationEvaluator<V> {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  internal suspend fun getStandardTestParameters(
    testContext: ArtifactInEnvironmentContext,
    testRunnerRegion: String? = null
  ): Map<String, String> {
    val fullArtifact = keelRepository.getArtifactVersion(
      artifact = testContext.artifact,
      version = testContext.version,
      status = null
    ) ?: error("No artifact details found for artifact reference ${testContext.artifactReference}" +
      " and version ${testContext.version} in config ${testContext.deliveryConfig.application}")

    val vipDns = eurekaVipDns(testContext, testRunnerRegion)
    val clusterDns = eurekaClusterDns(testContext, testRunnerRegion)
    val loadBalancerDns = loadBalancerDns(testContext, testRunnerRegion)

    return mutableMapOf(
      TEST_ENV.name to testContext.environmentName,
      TEST_REPO_URL.name to fullArtifact.repoUrl(gitLinkConfig.gitUrlPrefix),
      TEST_BUILD_NUMBER.name to "${fullArtifact.buildNumber}",
      TEST_ARTIFACT_VERSION.name to fullArtifact.version,
      TEST_BRANCH_NAME.name to "${fullArtifact.gitMetadata?.branch}",
      TEST_COMMIT_SHA.name to "${fullArtifact.gitMetadata?.commit}",
      TEST_COMMIT_URL.name to "${fullArtifact.gitMetadata?.commitInfo?.link}",
      TEST_PR_NUMBER.name to "${fullArtifact.gitMetadata?.pullRequest?.number}",
      TEST_PR_URL.name to "${fullArtifact.gitMetadata?.pullRequest?.url}",
    ).apply {
      if (vipDns != null) put(TEST_EUREKA_VIP.name, vipDns.address)
      if (clusterDns != null) put(TEST_EUREKA_CLUSTER.name, clusterDns.address)
      if (loadBalancerDns != null) put(TEST_LOAD_BALANCER.name, loadBalancerDns.address)
    }
  }

  private suspend fun clusterEndpoints(
    context: ArtifactInEnvironmentContext,
    testRunnerRegion: String? = null
  ) =
    context.deliveryConfig
      .resourcesUsing(context.artifactReference, context.environmentName)
      .find { it.spec is ComputeResourceSpec<*> }
      ?.let {
        networkEndpointProvider.getNetworkEndpoints(it).filter { endpoint ->
          testRunnerRegion == null || endpoint.region == testRunnerRegion
        }
      } ?: emptyList()

  private suspend fun eurekaVipDns(
    testContext: ArtifactInEnvironmentContext,
    testRunnerRegion: String? = null
  ): NetworkEndpoint? {
    val clusterVip = clusterEndpoints(testContext, testRunnerRegion).find { it.type == EUREKA_VIP_DNS }
    if (clusterVip == null) {
      log.debug("Unable to find cluster VIP endpoint for ${testContext.artifact} " +
        "in environment ${testContext.environmentName} of application ${testContext.deliveryConfig.application}")
    }
    return clusterVip
  }

  private suspend fun eurekaClusterDns(
    testContext: ArtifactInEnvironmentContext,
    testRunnerRegion: String? = null
  ): NetworkEndpoint? {
    val clusterEureka = clusterEndpoints(testContext, testRunnerRegion).find { it.type == EUREKA_CLUSTER_DNS }
    if (clusterEureka == null) {
      log.debug("Unable to find cluster Eureka endpoint for ${testContext.artifact} " +
        "in environment ${testContext.environmentName} of application ${testContext.deliveryConfig.application}")
    }
    return clusterEureka
  }

  private suspend fun loadBalancerDns(
    testContext: ArtifactInEnvironmentContext,
    testRunnerRegion: String? = null
  ): NetworkEndpoint? {
    val cluster = testContext.deliveryConfig
      .resourcesUsing(testContext.artifactReference, testContext.environmentName)
      .find { it.spec is ComputeResourceSpec<*> } as? Resource<Dependent>

    val clusterLoadBalancers = cluster?.spec?.dependsOn
      ?.filter { it.type == LOAD_BALANCER }
      ?.map { it -> it.name }
      ?: emptyList()

    val loadBalancerDns = if (cluster != null) {
      testContext.deliveryConfig.resources.find { resource ->
        resource.name in clusterLoadBalancers
      }?.let { loadBalancer ->
        networkEndpointProvider.getNetworkEndpoints(loadBalancer).find {
          testRunnerRegion == null || it.region == testRunnerRegion
        }
      }
    } else {
      null
    }

    if (clusterLoadBalancers.isNotEmpty() && loadBalancerDns == null) {
      log.debug("Unable to find load balancer endpoint for ${testContext.artifact} " +
        "in environment ${testContext.environmentName} of application ${testContext.deliveryConfig.application}")
    }

    return loadBalancerDns
  }
}
