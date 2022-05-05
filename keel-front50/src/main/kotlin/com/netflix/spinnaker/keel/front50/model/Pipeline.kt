package com.netflix.spinnaker.keel.front50.model

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnore
import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.core.api.SubmittedResource
import java.time.Instant

/**
 * A Spinnaker pipeline.
 */
data class Pipeline(
  val name: String,
  val id: String,
  val application: String,
  val disabled: Boolean = false,
  val fromTemplate: Boolean = false,
  val triggers: List<Trigger> = emptyList(),
  @JsonAlias("stages")
  private val _stages: List<Stage> = emptyList(),
  @JsonAlias("updateTs")
  private val _updateTs: Long? = null,
  val lastModifiedBy: String? = null,
  @JsonIgnore
  /* will hold resources we are able to export from the pipeline for migration purposes  */
  val resources: MutableSet<SubmittedResource<ResourceSpec>>? = mutableSetOf(),
  @JsonIgnore
  /* will hold constraints we are able to export from the pipeline for migration purposes */
  val constraints: MutableSet<Constraint>? = mutableSetOf(),
  @JsonIgnore
  /* will hold artifacts we are able to export from the pipeline for migration purposes*/
  val artifacts: MutableSet<DeliveryArtifact>? = mutableSetOf(),
  val parameterConfig: List<PipelineParameterConfig> = emptyList(),
  val notifications: List<PipelineNotifications> = emptyList()
) {
  /**
   * The pipeline stages, in the right order of dependency between them.
   */
  val stages: List<Stage>
    get() = _stages.sortedBy { stage ->
      predecessorsOf(stage).reversed().joinToString()
    }

  /**
   * @return The list of refIds corresponding to the predecessors of the [stage], recursively.
   */
  private fun predecessorsOf(stage: Stage): List<String> =
    stage.requisiteStageRefIds.flatMap { refId ->
      _stages.find { it.refId == refId }
        ?.let { listOf(it.refId) + predecessorsOf(it) }
        ?: emptyList()
    }

  /**
   * List of the pipeline stage types ordered by stage dependencies.
   */
  val shape: List<String>
    get() = if (hasParallelStages) listOf("<parallel-stages>") else stages.map { stage -> stage.type }

  val updateTs: Instant?
    get() = _updateTs?.let { Instant.ofEpochMilli(it) }

  val hasParallelStages: Boolean
    get() = stages.any { it.requisiteStageRefIds.size > 1 }

  fun findUpstreamBake(deployStage: DeployStage): BakeStage? {
    val i = stages.indexOf(deployStage)
    return stages.subList(0, i).filterIsInstance<BakeStage>().lastOrNull()
  }

  fun findDownstreamDeploys(stage: Stage): List<DeployStage> {
    val i = stages.indexOf(stage)
    return stages.slice(i until stages.size).filterIsInstance<DeployStage>()
  }

  fun findDeployForCluster(findImageStage: FindImageStage) =
    stages
      .filterIsInstance<DeployStage>()
      .find { deploy ->
        deploy.clusters.any { cluster ->
          findImageStage.cloudProvider == cluster.provider &&
            findImageStage.cluster == cluster.name &&
            findImageStage.credentials == cluster.account &&
            cluster.region in findImageStage.regions
        }
      }

  fun findDownstreamJenkinsStages(deployStage: DeployStage): List<JenkinsStage> {
    val i = stages.indexOf(deployStage)
    return stages.slice(i until stages.size).filterIsInstance<JenkinsStage>()
  }

  fun hasManualJudgment(deployStage: DeployStage) =
    try {
      stages[stages.indexOf(deployStage) - 1] is ManualJudgmentStage
    } catch (e: IndexOutOfBoundsException) {
      false
    }

  override fun equals(other: Any?) = if (other is Pipeline) {
    other.id == this.id
  } else {
    super.equals(other)
  }

  override fun hashCode() = id.hashCode()
}

/**
 * Searches the list of pipelines for one that contains a deploy stage matching the cluster described in the given
 * [FindImageStage]. Returns a pair of the pipeline and deploy stage, if found.
 */
fun List<Pipeline>.findPipelineWithDeployForCluster(findImageStage: FindImageStage): Pair<Pipeline, DeployStage>? {
  forEach { pipeline ->
    val deploy = pipeline.findDeployForCluster(findImageStage)
    if (deploy != null) {
      return pipeline to deploy
    }
  }
  return null
}

data class PipelineParameterConfig(
  val name: String,
  val default: String? = null,
)
