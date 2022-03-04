package com.netflix.spinnaker.keel.api.migration

enum class SkipReason {
  DISABLED, FROM_TEMPLATE, HAS_PARALLEL_STAGES, SHAPE_NOT_SUPPORTED, NOT_EXECUTED_RECENTLY, ARTIFACT_NOT_SUPPORTED, RESOURCE_NOT_SUPPORTED
}

/**
 * A simplified representation of pipelines that we failed to convert into a delivery config details
 */
data class SkippedPipeline(
  val id: String,
  val name: String,
  val shape: String,
  val link: String,
  val reason: SkipReason,
)
