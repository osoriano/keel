package com.netflix.spinnaker.keel.core.api

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.NotificationConfig
import com.netflix.spinnaker.keel.api.PreviewEnvironmentSpec
import com.netflix.spinnaker.keel.api.schema.SchemaIgnore
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.Verification
import com.netflix.spinnaker.keel.api.*
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.postdeploy.PostDeployAction
import com.netflix.spinnaker.keel.api.schema.Description
import com.netflix.spinnaker.keel.api.schema.Title
import com.netflix.spinnaker.keel.serialization.SubmittedEnvironmentDeserializer

const val DEFAULT_SERVICE_ACCOUNT = "keel@spinnaker.io"

@Title("Delivery Config")
@Description("A manifest specifying the environments and resources that comprise an application.")
data class SubmittedDeliveryConfig(
  val application: String,
  val name: String?,
  @Title("Service account")
  @Description("The service account Spinnaker will authenticate with when making changes.")
  val serviceAccount: String?,
  val artifacts: Set<DeliveryArtifact> = emptySet(),
  val environments: Set<SubmittedEnvironment> = emptySet(),
  @Title("Preview environment templates")
  @get:JsonInclude(JsonInclude.Include.NON_EMPTY)
  val previewEnvironments: Set<PreviewEnvironmentSpec> = emptySet(),
  @get:JsonInclude(JsonInclude.Include.NON_EMPTY)
  val metadata: Map<String, Any?>? = emptyMap(),
  @SchemaIgnore
  val rawConfig: String? = null
) {

  val safeName: String
    @JsonIgnore get() = name ?: "$application-manifest"

  fun toDeliveryConfig(): DeliveryConfig = DeliveryConfig(
    name = safeName,
    application = application,
    serviceAccount = serviceAccount
      ?: error("No service account specified for app ${application}, and no default applied"),
    artifacts = artifacts.mapTo(mutableSetOf()) { artifact ->
      artifact.withDeliveryConfigName(safeName)
    },
    environments = environments.mapTo(mutableSetOf()) { env ->
      env.toEnvironment(this)
    },
    previewEnvironments = previewEnvironments,
    metadata = metadata ?: emptyMap(),
    rawConfig = rawConfig
  )
}

@JsonDeserialize(using = SubmittedEnvironmentDeserializer::class)
@Title("Environment")
data class SubmittedEnvironment(
  val name: String,
  @Description("The environment's cloud infrastructure - clusters, security groups, graphql schemas, etc")
  val resources: Set<SubmittedResource<*>>,
  @get:JsonInclude(JsonInclude.Include.NON_EMPTY)
  val constraints: Set<Constraint> = emptySet(),
  @Title("Verifications")
  @get:JsonInclude(JsonInclude.Include.NON_EMPTY)
  val verifyWith: List<Verification> = emptyList(),
  @get:JsonInclude(JsonInclude.Include.NON_EMPTY)
  val notifications: Set<NotificationConfig> = emptySet(),
  @Title("Post deploy actions")
  @get:JsonInclude(JsonInclude.Include.NON_EMPTY)
  val postDeploy: List<PostDeployAction> = emptyList(),
  @get:JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Description("Optional locations that are propagated to any [resources] where they are not specified.")
  val locations: SubnetAwareLocations? = null,
  @Description("Optional workload type that will be used to determine [locations] where they are not specified.")
  val workloadType: WorkloadType? = null
) {
  // We declare the metadata field here such that it's not used in equals() and hashCode(), since we don't
  // care about the metadata when comparing environments.
  @get:JsonIgnore
  val metadata: MutableMap<String, Any?> = mutableMapOf()

  fun addMetadata(vararg metadata: Pair<String, Any?>) =
    apply {
      this.metadata.putAll(metadata)
    }

  fun toEnvironment(deliveryConfig: SubmittedDeliveryConfig, serviceAccount: String? = null) = Environment(
    name = name,
    resources = resources.mapTo(mutableSetOf()) { resource ->
      resource
        .copy(metadata = mapOf("serviceAccount" to serviceAccount) + resource.metadata)
        .normalize(deliveryConfig)
    },
    constraints = constraints,
    verifyWith = verifyWith,
    notifications = notifications,
    postDeploy = postDeploy
  ).addMetadata(metadata)
}
