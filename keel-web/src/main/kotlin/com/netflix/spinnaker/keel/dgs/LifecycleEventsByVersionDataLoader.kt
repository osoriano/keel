package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsDataLoader
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.graphql.types.MD_LifecycleEventScope
import com.netflix.spinnaker.keel.graphql.types.MD_LifecycleEventStatus
import com.netflix.spinnaker.keel.graphql.types.MD_LifecycleEventType
import com.netflix.spinnaker.keel.graphql.types.MD_LifecycleStep
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventRepository
import com.netflix.spinnaker.keel.lifecycle.LifecycleStep
import org.dataloader.MappedBatchLoader
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * Loads all lifecycle events for a single version of an artifact
 */
@DgsDataLoader(name = LifecycleEventsByVersionDataLoader.Descriptor.name)
class LifecycleEventsByVersionDataLoader(
  private val lifecycleEventRepository: LifecycleEventRepository
) : MappedBatchLoader<ArtifactAndVersion, List<MD_LifecycleStep>> {

  object Descriptor {
    const val name = "artifact-lifecycle-events-version"
  }

  override fun load(keys: MutableSet<ArtifactAndVersion>): CompletionStage<MutableMap<ArtifactAndVersion, List<MD_LifecycleStep>>> {
    return CompletableFuture.supplyAsync {
      val result: MutableMap<ArtifactAndVersion, List<MD_LifecycleStep>> = mutableMapOf()
      keys
        .map { it.artifact }
        .toSet()
        .forEach { artifact ->
          val allVersions: List<MD_LifecycleStep> = lifecycleEventRepository
            .getSteps(artifact)
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
      result
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
