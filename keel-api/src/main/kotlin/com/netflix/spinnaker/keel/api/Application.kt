package com.netflix.spinnaker.keel.api

import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL

/**
 * A Spinnaker application, as represented in Front50.
 */
@JsonInclude(NON_NULL)
data class Application(
  val name: String,
  val email: String? = null,
  val dataSources: DataSources? = null,
  val repoProjectKey: String? = null,
  val repoSlug: String? = null,
  val repoType: String? = null,
  val createTs: String? = null,
  val managedDelivery: ManagedDeliveryConfig? = null,
  val slackChannel: SlackChannel? = null,
  @get:JsonAnyGetter val details: MutableMap<String, Any?> = mutableMapOf()
) {
  @JsonAnySetter
  fun setDetail(key: String, value: Any?) {
    details[key] = value
  }

  val gitRepository: GitRepository
    get() = if (repoType != null && repoProjectKey != null && repoSlug != null) {
      GitRepository(repoType, repoProjectKey, repoSlug)
    } else {
      error("Git repository information missing from application config. Please fix it in Spinnaker.")
    }
}

data class DataSources(
  val enabled: List<String>,
  val disabled: List<String>
)

@JsonInclude(NON_NULL)
data class ManagedDeliveryConfig(
  val importDeliveryConfig: Boolean = false,
  val manifestPath: String? = null
)

@JsonInclude(NON_NULL)
data class SlackChannel (
  val name: String? = null
)
