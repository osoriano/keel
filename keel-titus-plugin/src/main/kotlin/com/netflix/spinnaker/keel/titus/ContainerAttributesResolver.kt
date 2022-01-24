package com.netflix.spinnaker.keel.titus

import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.plugins.Resolver
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.api.titus.TitusServerGroupSpec
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import kotlinx.coroutines.runBlocking
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component

/**
 * A [Resolver] that looks up the needed titus default container attributes and
 * populates them, if they are not specified by the user.
 */
@Component
@EnableConfigurationProperties(DefaultContainerAttributes::class)
class ContainerAttributesResolver(
  val defaults: DefaultContainerAttributes,
  val cloudDriverCache: CloudDriverCache,
  val springEnv: Environment
) : Resolver<TitusClusterSpec> {

  private val enabled: Boolean
    get() = springEnv.getProperty("keel.titus.resolvers.container-attributes.enabled", Boolean::class.java, true)

  override val supportedKind = TITUS_CLUSTER_V1

  override fun invoke(resource: Resource<TitusClusterSpec>): Resource<TitusClusterSpec> {
    if (!enabled) {
      return resource
    }

    val topLevelAttrs = resource.spec.defaults.containerAttributes?.toMutableMap() ?: mutableMapOf()
    val overrides = resource.spec.overrides.toMutableMap()

    val account = resource.spec.locations.account
    // account key will be the same for all regions, so add it to the top level
    if (!keyPresentInAllRegions(resource, defaults.getAccountKey())) {
      val awsAccountId = cloudDriverCache.getAwsAccountIdForTitusAccount(account)
      topLevelAttrs.putIfAbsent(defaults.getAccountKey(), awsAccountId)
    }

    if (!keyPresentInAllRegions(resource, defaults.getSubnetKey())) {
      val regions = resource.spec.locations.regions.map { it.name }
      regions.forEach { region ->
        var override = overrides[region] ?: TitusServerGroupSpec()
        val subnet = defaults.getSubnetValue(account, region)
        if (subnet != null) {
          // only add subnet pair if we have an entry for the subnet
          val subnetDefault = mapOf(defaults.getSubnetKey() to subnet)
          val containerAttributes = override.containerAttributes ?: mutableMapOf()
          override = if (!containerAttributes.containsKey(defaults.getSubnetKey())) {
            override.copy(containerAttributes = containerAttributes + subnetDefault)
          } else {
            override.copy(containerAttributes = containerAttributes)
          }
          overrides[region] = override
        }
      }
    }

    val containerEnv = cloudDriverCache.getAccountEnvironment(account)
    if (containerEnv == "test" && !keyPresentInAllRegions(resource, defaults.getIPv6Key())) {
      // Container tunables should be a true/false boolean string
      topLevelAttrs.putIfAbsent(defaults.getIPv6Key(), "true")
    }

    val resourceDefaults = resource.spec.defaults.copy(containerAttributes = topLevelAttrs)
    val newSpec = resource.spec.copy(overrides = overrides, _defaults = resourceDefaults)
    return resource.copy(spec = newSpec)
  }

  /**
   * Returns true if when resolved, each region contains the desired key
   */
  private fun keyPresentInAllRegions(resource: Resource<TitusClusterSpec>, key: String): Boolean {
    val regions = resource.spec.locations.regions.map { it.name }
    var allPresent = true
    regions.forEach{ region ->
      val resolvedAttrs = resource.spec.resolveContainerAttributes(region)
      if (!resolvedAttrs.containsKey(key)) {
        allPresent = false
      }
    }
    return allPresent
  }
}
