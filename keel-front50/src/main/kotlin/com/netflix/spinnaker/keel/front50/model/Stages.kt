package com.netflix.spinnaker.keel.front50.model

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.netflix.spinnaker.keel.api.artifacts.BaseLabel
import com.netflix.spinnaker.keel.api.artifacts.BaseLabel.RELEASE
import com.netflix.spinnaker.keel.api.artifacts.StoreType
import com.netflix.spinnaker.keel.api.artifacts.StoreType.EBS

/**
 * A stage in a Spinnaker [Pipeline].
 */
@JsonTypeInfo(
  use = Id.NAME,
  include = As.EXISTING_PROPERTY,
  property = "type",
  visible = true, // the default is false and hides the property from the deserializer!
  defaultImpl = GenericStage::class
)
@JsonSubTypes(
  Type(value = BakeStage::class, name = "bake"),
  Type(value = DeployStage::class, name = "deploy"),
  Type(value = FindImageStage::class, name = "findImage"),
  Type(value = FindImageFromTagsStage::class, name = "findImageFromTags"),
  Type(value = ManualJudgmentStage::class, name = "manualJudgment"),
  Type(value = JenkinsStage::class, name = "jenkins"),
  Type(value = ChapCanaryStage::class, name = "runChapCanary")
)
abstract class Stage {
  abstract val type: String
  abstract val name: String
  abstract val refId: String
  open val requisiteStageRefIds: List<String> = emptyList()

  @get:JsonAnyGetter
  val details: MutableMap<String, Any> = mutableMapOf()

  @JsonAnySetter
  fun setAttribute(key: String, value: Any) {
    details[key] = value
  }
}

data class GenericStage(
  override val name: String,
  override val type: String,
  override val refId: String,
  override val requisiteStageRefIds: List<String> = emptyList()
) : Stage()

data class BakeStage(
  override val name: String,
  override val refId: String,
  override val requisiteStageRefIds: List<String> = emptyList(),
  val `package`: String,
  val baseLabel: BaseLabel = RELEASE,
  val baseOs: String,
  val regions: Set<String>,
  val storeType: StoreType = EBS,
  val vmType: String = "hvm",
  val cloudProviderType: String = "aws"
) : Stage() {
  override val type = "bake"
}

data class DeployStage(
  override val name: String,
  override val refId: String,
  override val requisiteStageRefIds: List<String> = emptyList(),
  val clusters: Set<Cluster>,
  val restrictedExecutionWindow: RestrictedExecutionWindow? = null
) : Stage() {
  override val type = "deploy"
}

data class RestrictedExecutionWindow (
  val whitelist : List<TimeWindowConfig>? = null,
  val days: List<Int>? = null
)

data class TimeWindowConfig (
  val startHour: Int,
  val startMin: Int,
  val endHour: Int,
  val endMin: Int
)

enum class SelectionStrategy {
  LARGEST,
  NEWEST,
  OLDEST,
  FAIL
}

data class FindImageStage(
  override val name: String,
  override val refId: String,
  override val requisiteStageRefIds: List<String> = emptyList(),
  val cluster: String,
  val credentials: String, // account
  val onlyEnabled: Boolean,
  val selectionStrategy: SelectionStrategy,
  val cloudProvider: String,
  val regions: Set<String>,
  val cloudProviderType: String = cloudProvider
) : Stage() {
  override val type = "findImage"
}

data class FindImageFromTagsStage(
  override val name: String,
  override val refId: String,
  override val requisiteStageRefIds: List<String> = emptyList(),
) : Stage() {
  override val type = "findImageFromTags"
}

data class ManualJudgmentStage(
  override val name: String,
  override val refId: String,
  override val requisiteStageRefIds: List<String> = emptyList(),
) : Stage()  {
  override val type = "manualJudgment"
}

data class JenkinsStage(
  override val name: String,
  override val refId: String,
  @JsonAlias("master")
  val controller: String,
  val job: String,
  val parameters: Map<String, String> = emptyMap(),
  override val requisiteStageRefIds: List<String> = emptyList(),
) : Stage() {
  override val type = "jenkins"
}

data class ChapCanaryStage(
  override val name: String,
  override val refId: String,
  override val requisiteStageRefIds: List<String> = emptyList(),
  val pipelineParameters: ChapCanaryParameters
) : Stage() {
  override val type = "runChapCanary"
}

data class ChapCanaryParameters(
  val app: String,
  val baselinePropertyOverrides: Map<String, String> = emptyMap(),
  val configProperties: Map<String, String> = emptyMap(),
  val cluster: String = "",
  val clusterTag: String? = null,
  val constraints: ChapCanaryConstraints,
  val durationInMinutes: Any,
  val env: String,
  val global: Boolean = false,
  val intervalInMinutes: Any,
  val kayentaConfigIds: List<String>,
  val numberOfInstances: Any?,
  val regionMetadata: List<ChapCanaryRegionMetadata>,
  val slidingWindow: Boolean = false,
  val slidingWindowSize: Int?,
  val warmupInMinutes: Any = 0
)

data class ChapCanaryRegionMetadata(
  val imageId: String? = null,
  val region: String,
  val startDelayInMinutes: Int? = null,
)

data class ChapCanaryConstraints(
  val canaryAnalysisThresholds: ChapCanaryAnalysisThresholds,
  val resultStrategy: ChapCanaryResultStrategy
)

enum class ChapCanaryResultStrategy {
  LOWEST, AVERAGE
}

data class ChapCanaryAnalysisThresholds(
  val marginal: Int,
  val pass: Int
)

