package com.netflix.spinnaker.keel.resolvers

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.plugins.Resolver
import com.netflix.spinnaker.keel.core.ResourceCurrentlyUnresolvable
import com.netflix.spinnaker.keel.persistence.KeelRepository

/**
 * Base class for resolving an image.
 * Shared version selecting logic between ec2 and titus
 */
abstract class AbstractImageResolver <T : ResourceSpec>(
  open val repository: KeelRepository,
  open val featureToggles: FeatureToggles
) : Resolver<T> {

  /**
   * This is where we choose what version should be in the desired state.
   * This takes into account approved versions. 
   */
  fun getLatestVersion(
    deliveryConfig: DeliveryConfig,
    environment: Environment,
    artifact: DeliveryArtifact
  ): String {
   return repository.latestVersionApprovedIn(
        deliveryConfig,
        artifact,
        environment.name
      ) ?: throw NoApprovedVersionForEnvironment(artifact, environment.name)
  }
}

class NoApprovedVersionForEnvironment(
  val artifact: DeliveryArtifact,
  val environment: String
) : ResourceCurrentlyUnresolvable("No version found for artifact ${artifact.name} (ref: ${artifact.reference}) that satisfies constraints in environment $environment. Are there any approved versions?")
