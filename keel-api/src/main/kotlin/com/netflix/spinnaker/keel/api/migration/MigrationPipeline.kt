package com.netflix.spinnaker.keel.api.migration

import com.netflix.spinnaker.keel.api.Constraint

enum class SkipReason {
  DISABLED, FROM_TEMPLATE, HAS_PARALLEL_STAGES, SHAPE_NOT_SUPPORTED, NOT_EXECUTED_RECENTLY, ARTIFACT_NOT_SUPPORTED, RESOURCE_NOT_SUPPORTED
}

enum class PipelineStatus {
  PROCESSED, EXPORTED, NOT_SUPPORTED, SKIPPED
}

data class OrphanResources (
  val id: String,
  val name: String,
  val resourceSpec: Map<String, Any?> = emptyMap()
  )

data class OrphanConstraints (
  val type: Constraint,
  val name: String,
  val constraintSpec: Map<String, Any?> = emptyMap()
)

/**
 * A simplified representation of all pipelines we processed, wheter succesfully or not
 */
data class MigrationPipeline(
  val id: String,
  val name: String,
  val shape: String,
  val link: String,
  val reason: SkipReason? = null,
  val status: PipelineStatus,
  val environments: List<String>? = emptyList(),
  val orphanResources: List<OrphanResources>? = emptyList(),
  val orphanConstraints: List<OrphanConstraints>? = emptyList(),
)
