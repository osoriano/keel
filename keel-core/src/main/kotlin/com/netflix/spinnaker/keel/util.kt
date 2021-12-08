package com.netflix.spinnaker.keel

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.jackson.readValueInliningAliases
import com.netflix.spinnaker.kork.dynamicconfig.DynamicConfigService

inline fun <reified T : Any> DynamicConfigService.getConfig(configName: String, defaultValue: T): T =
  getConfig(T::class.java, configName, defaultValue)

fun YAMLMapper.parseDeliveryConfig(rawDeliveryConfig: String): SubmittedDeliveryConfig {
  return readValueInliningAliases<SubmittedDeliveryConfig>(rawDeliveryConfig)
    .copy(rawConfig = rawDeliveryConfig)
}

fun getPrDescription(user: String) = "Managed Delivery is a new delivery experience that allows you to spend more time developing your app, and less time on delivery and infrastructure. We recommend upgrading by merging this PR.\n" +
  "\n" +
  "\n" +
  "### Why upgrade?\n" +
  "- **Focus on commits, not pipelines** - Track your commits flowing through environments until they reach production. Questions like \"what's running in prod?\" and \"where's my commit?\" become simple to answer.\n" +
  "- **Delivery and infrastructure as code** - Managing delivery and infrastructure as part of your codebase provides better understanding of changes over time, simplifies tedious config updates, and improves team collaboration.\n" +
  "- **It's reversible**, and won't affect your current deployments.\n" +
  "\n" +
  "\n" +
  "### How it works\n" +
  "Your infrastructure and delivery configuration will be stored in a file called spinnaker.yml. It has three important components:\n" +
  "- **Resources** - Infrastructure components such as EC2/titus clusters, security groups, load balancers and DGS schemas.\n" +
  "- **Environments** - Logical grouping of resources which describe how your code flows from a build to production.\n" +
  "- **Constraints** - Gates that control when a new code version is deployed into an environment, such as manual judgement, canary or a dependency on another environment.\n" +
  "\n" +
  "You can read more about it in our docs: https://go/mddocs.\n" +
  "\n" +
  "*This PR was opened because $user started the upgrade process in Spinnaker.*\n"
