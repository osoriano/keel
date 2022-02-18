package com.netflix.spinnaker.keel.verification.container

import com.netflix.spinnaker.config.GitLinkConfig
import com.netflix.spinnaker.config.PromoteJarConfig
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.action.ActionState
import com.netflix.spinnaker.keel.api.plugins.VerificationEvaluator
import com.netflix.spinnaker.keel.network.NetworkEndpointProvider
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.titus.ContainerRunner
import com.netflix.spinnaker.keel.titus.LinkStrategy
import com.netflix.spinnaker.keel.verification.BaseVerificationEvaluator
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.stereotype.Component

/**
 * A [VerificationEvaluator] that runs a test container to verify an environment.
 *
 * @param linkStrategy because links associated with test containers will have details that depend on the specific
 * environment where keel is deployed, we optionally inject a [LinkStrategy] that contains the logic for calculating the URL.
 *
 */
@Component
@EnableConfigurationProperties(PromoteJarConfig::class)
class TestContainerVerificationEvaluator(
  private val linkStrategy: LinkStrategy? = null,
  private val containerRunner: ContainerRunner,
  override val keelRepository: KeelRepository,
  override val gitLinkConfig: GitLinkConfig,
  override val networkEndpointProvider: NetworkEndpointProvider
) : BaseVerificationEvaluator<TestContainerVerification>(keelRepository, gitLinkConfig, networkEndpointProvider) {

  override val supportedVerification: Pair<String, Class<TestContainerVerification>> =
    TestContainerVerification.TYPE to TestContainerVerification::class.java

  override suspend fun evaluate(
    context: ArtifactInEnvironmentContext,
    verification: TestContainerVerification,
    oldState: ActionState
  ): ActionState {
    log.debug("Getting new verification state for ${context.shortName()}")
    return containerRunner.getNewState(oldState, linkStrategy)
  }

  override suspend fun start(context: ArtifactInEnvironmentContext, verification: TestContainerVerification): Map<String, Any?> {
    val deliveryArtifact = context.deliveryConfig.matchingArtifactByReference(context.artifactReference)
    requireNotNull(deliveryArtifact) { "Artifact reference (${context.artifactReference}) in config (${context.deliveryConfig.application}) must correspond to a valid artifact" }

    return containerRunner.launchContainer(
      imageId = verification.imageId,
      description = "Verifying ${context.version} in environment ${context.environmentName} with test container ${verification.imageId}",
      serviceAccount = context.deliveryConfig.serviceAccount,
      application = context.deliveryConfig.application,
      containerApplication = verification.application ?: context.deliveryConfig.application,
      environmentName = context.environmentName,
      location = verification.location,
      entrypoint = verification.entrypoint ?: "",
      environmentVariables = getStandardTestParameters(context, verification.location.region) + verification.env
    )
  }

  private val log by lazy { LoggerFactory.getLogger(javaClass) }
}
