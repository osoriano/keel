package com.netflix.spinnaker.keel.exceptions

import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.kork.exceptions.SystemException

/**
 * Exception thrown when it's not safe to take action against the environment because
 * something is already acting on it.
 */
open class EnvironmentCurrentlyBeingActedOn(message: String) : SystemException(message)

class ActiveVerifications(val active: Collection<ArtifactInEnvironmentContext>, deliveryConfig: DeliveryConfig, environment: Environment) :
  EnvironmentCurrentlyBeingActedOn("active verifications in ${deliveryConfig.name} ${environment.name} against versions ${active.map {it.version}}")

class ActiveDeployments(deliveryConfig: DeliveryConfig, environment: Environment) :
  EnvironmentCurrentlyBeingActedOn("currently deploying into ${deliveryConfig.name} ${environment.name}")
