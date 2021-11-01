package com.netflix.spinnaker.keel.titus

import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.plugins.Resolver
import com.netflix.spinnaker.keel.api.plugins.SupportedKind
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.optics.resourceSpecLens
import com.netflix.spinnaker.keel.titus.optics.titusClusterSpecAccountLens
import com.netflix.spinnaker.keel.titus.optics.titusClusterSpecIamProfileLens
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class TitusIAMProfileResolver(
  private val cloudDriverCache: CloudDriverCache
) : Resolver<TitusClusterSpec> {

  override val supportedKind: SupportedKind<TitusClusterSpec> = TITUS_CLUSTER_V1

  override fun invoke(resource: Resource<TitusClusterSpec>): Resource<TitusClusterSpec> {
    val application = resource.application
    val account = resourceAccountLens.get(resource)
    val iamProfile = resourceIamProfileLens.get(resource)
    val updated = when {
      iamProfile == null -> "arn:aws:iam::${accountIdFor(account)}:role/${application}InstanceProfile"
      iamProfile.startsWith("arn:aws:iam::") -> iamProfile
      else -> "arn:aws:iam::${accountIdFor(account)}:role/${iamProfile}"
    }
    return resourceIamProfileLens.set(resource, updated)
  }

  private fun accountIdFor(account: String) =
    cloudDriverCache.awsAccountIdForTitusAccount(account).also {
      log.debug("Account id for {} is {}", account, it)
    }

  private val resourceAccountLens =
    resourceSpecLens<TitusClusterSpec>() + titusClusterSpecAccountLens

  private val resourceIamProfileLens =
    resourceSpecLens<TitusClusterSpec>() + titusClusterSpecIamProfileLens

  private val log by lazy { LoggerFactory.getLogger(javaClass) }
}
