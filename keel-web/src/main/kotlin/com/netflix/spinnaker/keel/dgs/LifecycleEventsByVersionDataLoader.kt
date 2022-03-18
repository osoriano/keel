package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsDataLoader
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.graphql.types.MD_LifecycleEventScope
import com.netflix.spinnaker.keel.graphql.types.MD_LifecycleEventStatus
import com.netflix.spinnaker.keel.graphql.types.MD_LifecycleEventType
import com.netflix.spinnaker.keel.graphql.types.MD_LifecycleStep
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventRepository
import com.netflix.spinnaker.keel.lifecycle.LifecycleStep
import com.netflix.springboot.scheduling.DefaultExecutor
import org.dataloader.MappedBatchLoader
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executor

/**
 * Loads all lifecycle events for a single version of an artifact
 */
@DgsDataLoader(name = LifecycleEventsByVersionDataLoader.Descriptor.name)
class LifecycleEventsByVersionDataLoader(
  private val lifecycleEventRepository: LifecycleEventRepository,
  @DefaultExecutor private val executor: Executor
) : MappedBatchLoader<ArtifactAndVersion, List<MD_LifecycleStep>> {

  object Descriptor {
    const val name = "artifact-lifecycle-events-version"
  }

  fun loadData(keys: MutableSet<ArtifactAndVersion>): MutableMap<ArtifactAndVersion, List<MD_LifecycleStep>> {
    val result: MutableMap<ArtifactAndVersion, List<MD_LifecycleStep>> = mutableMapOf()
    keys.groupBy { it.artifact }.entries.forEach { (artifact, entries) ->
      val allVersions: List<MD_LifecycleStep> = lifecycleEventRepository
        .getSteps(artifact, entries.map { it.version })
        .map { it.toDgs() }

      val byVersion: Map<String, List<MD_LifecycleStep>> = allVersions
        .filter { it.artifactVersion != null }
        .groupBy { it.artifactVersion!! }

      result.putAll(
        byVersion.mapKeys { entry ->
          ArtifactAndVersion(artifact, entry.key)
        }
      )
    }
    return result
  }

  override fun load(keys: MutableSet<ArtifactAndVersion>): CompletionStage<MutableMap<ArtifactAndVersion, List<MD_LifecycleStep>>> {
    return executor.supplyAsync {
      loadData(keys)
    }
  }
}

fun LifecycleStep.toDgs() =
  MD_LifecycleStep(
    scope = MD_LifecycleEventScope.valueOf(scope.name),
    type = MD_LifecycleEventType.valueOf(type.name),
    id = id,
    status = MD_LifecycleEventStatus.valueOf(status.name),
    text = text,
    link = link,
    startedAt = startedAt,
    completedAt = completedAt,
    artifactVersion = artifactVersion,
  )

data class ArtifactAndVersion(
  val artifact: DeliveryArtifact,
  val version: String
)
