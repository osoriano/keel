package com.netflix.spinnaker.keel.titus

import arrow.optics.Lens
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.plugins.Resolver
import com.netflix.spinnaker.keel.api.titus.NetworkMode
import com.netflix.spinnaker.keel.api.titus.NetworkMode.Ipv4Only
import com.netflix.spinnaker.keel.api.titus.NetworkMode.Ipv6AndIpv4
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.api.titus.TitusServerGroupSpec
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.core.api.DEFAULT_SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.titus.exceptions.AwsAccountConfigurationException
import com.netflix.spinnaker.keel.titus.optics.titusClusterSpecDefaultsLens
import com.netflix.spinnaker.keel.titus.optics.titusClusterSpecOverridesLens
import com.netflix.spinnaker.keel.titus.optics.titusServerGroupSpecContainerAttributesLens
import kotlinx.coroutines.runBlocking
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component

/**
 * This resolver sets the networkMode field and removes the IPv6 container attribute (which was deprecated in Titus,
 * in both the "defaults" TitusServerGroupSpec and for each of the "overrides" TitusServerGroupSpecs
 */

@Component
@EnableConfigurationProperties(DefaultContainerAttributes::class)
class NetworkModeResolver(
  val defaults: DefaultContainerAttributes,
  val springEnv: Environment,
  val cloudDriverService: CloudDriverService
) : Resolver<TitusClusterSpec> {
  override val supportedKind = TITUS_CLUSTER_V1

  private val enabled: Boolean
    get() = springEnv.getProperty("keel.titus.resolvers.network-mode.enabled", Boolean::class.java, true)

  override fun invoke(resource: Resource<TitusClusterSpec>): Resource<TitusClusterSpec> {
    if (!enabled) {
      return resource
    }

    val account = resource.spec.locations.account

    val spec = resource.spec
      .resolveDefaults(account)
      .resolveOverrides(account)

    return resource.copy(spec=spec)
  }

  /**
   * Resolve a TitusServerGroupSpec to use networkMode instead of container attributes for specifying
   * IPv4/IPv6 details
   *
   * The resolved spec:
   *  - always has a non-null networkMode field
   *  - never contains the old-style IPv6 container attribute
   */
  fun TitusServerGroupSpec.resolve(titusAccount: String): TitusServerGroupSpec {
    val networkMode = getNetworkMode(titusAccount)

    val result = titusClusterSpecDefaultsNetworkModeLens.set(this, networkMode)

    return titusServerGroupSpecContainerAttributesLens.set(result, removeIpv6Attribute(containerAttributes))
  }

  /**
   * Resolve the "defaults" TitusServerGroupSpec object
   */
  private fun TitusClusterSpec.resolveDefaults(account: String): TitusClusterSpec =
    titusClusterSpecDefaultsLens.modify(this) { it.resolve(account) }

  /**
   * Resolve the per-region "overrides" TitusServerGroupSpec objects
   */
  private fun TitusClusterSpec.resolveOverrides(account: String): TitusClusterSpec =
    titusClusterSpecOverridesLens.modify(this) {
      it.mapValues { (_, serverGroupSpec) -> serverGroupSpec.resolve(account) }
    }

  /**
   * Determine the NetworkMode
   *
   *  - If the user has explicitly specified the networkMode field, just use that
   *  - If the user has specified the old-style IPv6 container attribute flag, use that to determine the mode
   *  - Otherwise, default to Dual Stack in test, v4 only in prod
   *
   * @param account  the Titus account, which is used to determine whether this resource is in the "test" environment
   */
  private fun TitusServerGroupSpec.getNetworkMode(account: String): NetworkMode {
    val ipv6Attr = containerAttributes?.get(defaults.getIPv6Key())

    return when {
      networkMode != null -> networkMode!!
      ipv6Attr == "true" -> Ipv6AndIpv4
      ipv6Attr == "false" ->Ipv4Only
      inTestEnv(account) -> Ipv6AndIpv4
      else -> Ipv4Only
    }
  }

  /**
   * Returns true if the specified titus Account is in a "test" environment according to clouddriver
   *
   * Note: this kind of "environment" is unrelated to MD environments
   */
  private fun inTestEnv(titusAccount: String) =
    getAccountEnvironment(titusAccount) == "test"


  private fun getAccountEnvironment(titusAccount: String): String = runBlocking {
    cloudDriverService.getAccountInformation(titusAccount, DEFAULT_SERVICE_ACCOUNT)["environment"]?.toString()
      ?: throw AwsAccountConfigurationException(titusAccount, "environment")
  }

  private val titusClusterSpecDefaultsNetworkModeLens: Lens<TitusServerGroupSpec, NetworkMode?> = Lens(
    get = TitusServerGroupSpec::networkMode,
    set = { spec, networkMode -> spec.copy(networkMode = networkMode) }
  )

  /**
   * Remove the IPv6 attribute from a map of attributes
   */
  fun removeIpv6Attribute(attributes: Map<String, String>?): Map<String, String> =
    attributes?.toMutableMap()
      ?.apply { remove(defaults.getIPv6Key()) }
      ?: emptyMap()
}
