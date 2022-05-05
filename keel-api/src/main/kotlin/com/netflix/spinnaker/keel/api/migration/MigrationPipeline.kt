package com.netflix.spinnaker.keel.api.migration

enum class SkipReason {
  DISABLED, FROM_TEMPLATE, HAS_PARALLEL_STAGES, SHAPE_NOT_SUPPORTED, NOT_EXECUTED_RECENTLY, ARTIFACT_NOT_SUPPORTED, RESOURCE_NOT_SUPPORTED
}

enum class PipelineStatus {
  PROCESSED, EXPORTED, SKIPPED
}

data class PipelineResource (
  val id: String,
  val kind: String,
  val spec: Map<String, Any?> = emptyMap()
)

data class PipelineConstraint (
  val type: String,
  val spec: Map<String, Any?> = emptyMap()
)

data class PipelineArtifact (
  val type: String,
  val name: String,
  val spec: Map<String, Any?> = emptyMap()
)


/**
 * A simplified representation of all pipelines we processed, whether successfully or not
 */
data class MigrationPipeline(
  val id: String,
  val name: String,
  val shape: String,
  val link: String,
  val reason: SkipReason? = null,
  val status: PipelineStatus,
  val environments: List<String>? = emptyList(),
  val resources: Set<PipelineResource>? = emptySet(),
  val constraints: Set<PipelineConstraint>? = emptySet(),
  val artifacts: Set<PipelineArtifact>? = emptySet()
)
