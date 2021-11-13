package com.netflix.spinnaker.keel.api.migration

/**
 * A simplified representation of pipelines that we failed to convert into a delivery config details
 */
data class SkippedPipeline(
  val id: String,
  val name: String,
  val shape: String,
  val link: String,
  val reason: String,
)
