package com.netflix.spinnaker.keel.api.plugins

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.DeployableResourceSpec
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact

/**
 * A [ResourceHandler] for resources that can be the target of artifact deployments.
 */
interface DeployableResourceHandler<SPEC : DeployableResourceSpec, RESOLVED : Any> : ResourceHandler<SPEC, RESOLVED> {
  /**
   * Deploy the specified [artifact] to the specified [resource] in the specified [environment].
   *
   * TODO: in the future, it'd be nice to have all deployable handlers implement this function -- we could remove
   *  a ton of redundant code to find the currently approved version of the artifact, etc.
   */
  suspend fun deploy(deliveryConfig: DeliveryConfig, environment: Environment, resource: Resource<SPEC>, artifact: PublishedArtifact) {
    TODO("Not yet implemented")
  }

  /**
   * Redeploy the specified [resource] in the [environment] with the current version of its artifact.
   */
  suspend fun redeploy(deliveryConfig: DeliveryConfig, environment: Environment, resource: Resource<SPEC>)
}
