package com.netflix.spinnaker.keel.export

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.migration.PipelineArtifact
import com.netflix.spinnaker.keel.api.migration.PipelineConstraint
import com.netflix.spinnaker.keel.api.migration.PipelineResource
import com.netflix.spinnaker.keel.api.migration.SkipReason
import com.netflix.spinnaker.keel.api.migration.MigrationPipeline
import com.netflix.spinnaker.keel.api.migration.PipelineStatus
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.core.api.SubmittedEnvironment
import com.netflix.spinnaker.keel.core.api.SubmittedResource
import com.netflix.spinnaker.keel.core.api.id
import com.netflix.spinnaker.keel.exceptions.TagToReleaseArtifactException
import com.netflix.spinnaker.keel.front50.model.Pipeline
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper

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
  val exported: Map<Pipeline, List<SubmittedEnvironment>>,
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
    val objectMapper = configuredObjectMapper()
  }

  val pipelines: List<MigrationPipeline> = toMigratablePipelines()

  val exportSucceeded: Boolean
    get() = skipped.filterValues { !(VALID_SKIP_REASONS.contains(it)) }.isEmpty()
      //if the generated config is empty, fail the export as well
      && deliveryConfig.environments.isNotEmpty()

  val isInactive: Boolean
    get() = !exportSucceeded &&
      skipped.all { (_, reason) -> reason == SkipReason.DISABLED || reason == SkipReason.NOT_EXECUTED_RECENTLY }
}


fun PipelineExportResult.toMigratablePipelines(): List<MigrationPipeline> =
  skipped.map { (pipeline, reason) ->
    MigrationPipeline(
      id = pipeline.id,
      name = pipeline.name,
      link = pipeline.link(baseUrl),
      shape = pipeline.shape.joinToString(" -> "),
      reason = reason,
      status = PipelineStatus.PROCESSED,
      constraints = pipeline.constraints.toMigratableConstraints(),
      resources = pipeline.resources.toMigratableResouces(),
      artifacts = pipeline.artifacts.toMigratableArtifacts()
    )
  } +
    (exported.keys - skipped.keys).map { pipeline ->
        val environments = exported[pipeline]
        MigrationPipeline(
          name =  pipeline.name,
          link =  pipeline.link(baseUrl),
          shape =  pipeline.shape.joinToString(" -> "),
          environments =  environments?.map { it.name },
          id = pipeline.id,
          artifacts = pipeline.artifacts.toMigratableArtifacts(),
          status = PipelineStatus.EXPORTED
        )
      }

private fun Set<Constraint>?.toMigratableConstraints(): Set<PipelineConstraint>? {
  val objectMapper = configuredObjectMapper()
  return this?.map { constraint ->
    PipelineConstraint (
      type = constraint.type,
      spec = objectMapper.convertValue(constraint)
      )
  }?.toSet()
}

private fun Set<SubmittedResource<ResourceSpec>>?.toMigratableResouces(): Set<PipelineResource>? {
  val objectMapper = configuredObjectMapper()
  return this?.map { resource ->
    PipelineResource (
      id = resource.id,
      kind = resource.kind.kind,
      spec = objectMapper.convertValue(resource)
    )
  }?.toSet()
}

private fun Set<DeliveryArtifact>?.toMigratableArtifacts(): Set<PipelineArtifact>? {
  val objectMapper = configuredObjectMapper()
  return this?.map { artifact ->
    PipelineArtifact (
      name = artifact.name,
      type = artifact.type,
      spec = objectMapper.convertValue(artifact),
      warning = when(artifact.exportWarning) {
        is TagToReleaseArtifactException -> SkipReason.TAG_TO_RELEASE_ARTIFACT
        else -> null
      }
    )
  }?.toSet()
}


fun Pipeline.link(baseUrl: String) = "$baseUrl/#/applications/${application}/executions/configure/${id}"

data class UnsupportedJenkinsStage(override val message: String) : Exception(message)
