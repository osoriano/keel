package com.netflix.spinnaker.keel.export

import com.fasterxml.jackson.annotation.JsonIgnore
import com.netflix.spinnaker.keel.api.migration.SkipReason
import com.netflix.spinnaker.keel.api.migration.SkippedPipeline
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.core.api.SubmittedEnvironment
import com.netflix.spinnaker.keel.front50.model.Pipeline

interface ExportResult

data class ExportSkippedResult(
  val isManaged: Boolean
) : ExportResult

data class ExportErrorResult(
  val error: String,
  val detail: Any? = null
) : Exception(error, detail as? Throwable), ExportResult

data class PipelineExportResult(
  val deliveryConfig: SubmittedDeliveryConfig,
  val configValidationException: String?,
  @JsonIgnore
  val processed: Map<Pipeline, List<SubmittedEnvironment>>,
  @JsonIgnore
  val skipped: Map<Pipeline, SkipReason>,
  @JsonIgnore
  val baseUrl: String,
  @JsonIgnore
  val repoSlug: String?,
  @JsonIgnore
  val projectKey: String?
) : ExportResult {
  companion object {
    val VALID_SKIP_REASONS = listOf(SkipReason.DISABLED, SkipReason.NOT_EXECUTED_RECENTLY)
  }

  val pipelines: Map<String, Any> = mapOf(
    "exported" to (processed.keys - skipped.keys)
      .map { pipeline ->
        val environments = processed[pipeline]
        mapOf(
          "name" to pipeline.name,
          "link" to pipeline.link(baseUrl),
          "shape" to pipeline.shape.joinToString(" -> "),
          "environments" to environments?.map { it.name }
        )
      },
    "skipped" to skipped.map { (pipeline, reason) ->
      mapOf(
        "name" to pipeline.name,
        "link" to pipeline.link(baseUrl),
        "shape" to pipeline.shape.joinToString(" -> "),
        "reason" to reason
      )
    }
  )

  val exportSucceeded: Boolean
    get() = skipped.filterValues { !(VALID_SKIP_REASONS.contains(it)) }.isEmpty()
      //if the generated config is empty, fail the export as well
      && deliveryConfig.environments.isNotEmpty()

  val isInactive: Boolean
    get() = !exportSucceeded &&
      skipped.all { (_, reason) -> reason == SkipReason.DISABLED || reason == SkipReason.NOT_EXECUTED_RECENTLY }
}

fun PipelineExportResult.toSkippedPipelines(): List<SkippedPipeline> =
  skipped.map { (pipeline, reason) ->
    SkippedPipeline(
      id = pipeline.id,
      name = pipeline.name,
      link = pipeline.link(baseUrl),
      shape = pipeline.shape.joinToString(" -> "),
      reason = reason
    )
  }

fun Pipeline.link(baseUrl: String) = "$baseUrl/#/applications/${application}/executions/configure/${id}"
