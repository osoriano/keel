package com.netflix.spinnaker.keel.export

import com.fasterxml.jackson.annotation.JsonIgnore
import com.netflix.spinnaker.keel.api.migration.SkipReason
import com.netflix.spinnaker.keel.api.migration.MigrationPipeline
import com.netflix.spinnaker.keel.api.migration.PipelineStatus
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

  val pipelines: List<MigrationPipeline> = toMigratblePipelines()

  val exportSucceeded: Boolean
    get() = skipped.filterValues { !(VALID_SKIP_REASONS.contains(it)) }.isEmpty()
      //if the generated config is empty, fail the export as well
      && deliveryConfig.environments.isNotEmpty()

  val isInactive: Boolean
    get() = !exportSucceeded &&
      skipped.all { (_, reason) -> reason == SkipReason.DISABLED || reason == SkipReason.NOT_EXECUTED_RECENTLY }
}


fun PipelineExportResult.toMigratblePipelines(): List<MigrationPipeline> =
  skipped.map { (pipeline, reason) ->
    MigrationPipeline(
      id = pipeline.id,
      name = pipeline.name,
      link = pipeline.link(baseUrl),
      shape = pipeline.shape.joinToString(" -> "),
      reason = reason,
      status = PipelineStatus.PROCESSED,
      /** TODO[gyardeni]: implement this:
       orphanConstraints = emptyList(),
       orphanResources = emptyList()
      */
    )
  } +
    (processed.keys - skipped.keys).map { pipeline ->
        val environments = processed[pipeline]
        MigrationPipeline(
          name =  pipeline.name,
          link =  pipeline.link(baseUrl),
          shape =  pipeline.shape.joinToString(" -> "),
          environments =  environments?.map { it.name },
          id = pipeline.id,
          status = PipelineStatus.EXPORTED
        )
      }

fun Pipeline.link(baseUrl: String) = "$baseUrl/#/applications/${application}/executions/configure/${id}"

data class UnsupportedJenkinsStage(override val message: String) : Exception(message)
