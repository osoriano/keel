package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsDataLoader
import com.netflix.graphql.dgs.context.DgsContext
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.core.api.PublishedArtifactInEnvironment
import com.netflix.spinnaker.keel.graphql.types.MD_ArtifactStatusInEnvironment
import com.netflix.spinnaker.keel.graphql.types.MD_ArtifactVersionInEnvironment
import com.netflix.spinnaker.keel.persistence.KeelRepository
import org.dataloader.BatchLoaderEnvironment
import org.dataloader.MappedBatchLoaderWithContext
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * Loads all the version data for an artifacts in an environment.
 * This includes details about the version as well as the status in the specified environment.
 */
@DgsDataLoader(name = ArtifactInEnvironmentDataLoader.Descriptor.name)
class ArtifactInEnvironmentDataLoader(
  private val keelRepository: KeelRepository
) : MappedBatchLoaderWithContext<ArtifactAndEnvironment, List<MD_ArtifactVersionInEnvironment>> {

  object Descriptor {
    const val name = "artifact-in-environment"
  }

  override fun load(keys: MutableSet<ArtifactAndEnvironment>, environment: BatchLoaderEnvironment):
    CompletionStage<MutableMap<ArtifactAndEnvironment, List<MD_ArtifactVersionInEnvironment>>> {

    val applicationContext: ApplicationContext = DgsContext.getCustomContext(environment)
    return CompletableFuture.supplyAsync {
      val requestedStatuses = applicationContext.requestedStatuses
      val requestedVersionIds = applicationContext.requestedVersionIds
      val requestedLimit = applicationContext.requestedLimit
      val allVersions = keys
        .associateWith { key ->
          keelRepository
            .getAllVersionsForEnvironment(key.artifact, applicationContext.getConfig(), key.environmentName)
        }
      applicationContext.allVersions = allVersions

      allVersions.mapValues { (_, versions) ->
        versions
          .sortedByDescending { it.publishedArtifact.createdAt }
          .map { it.toDgs() }
          .filter {
            (
              requestedStatuses.isNullOrEmpty()
                || requestedStatuses.contains(it.status)
                || (requestedStatuses.contains(MD_ArtifactStatusInEnvironment.CURRENT) && it.isCurrent == true)
              ) &&
              (requestedVersionIds.isNullOrEmpty() || requestedVersionIds.contains(it.version))
          }.let {
            if (requestedLimit != null) {
              it.take(requestedLimit)
            } else {
              it
            }
          }
      }.toMutableMap()
    }
  }
}

//empty list means give me all statuses
data class ArtifactAndEnvironment(
  val artifact: DeliveryArtifact,
  val environmentName: String,
)

fun PublishedArtifactInEnvironment.toDgs() =
  MD_ArtifactVersionInEnvironment(
    id = "${environmentName}-${publishedArtifact.reference}-${publishedArtifact.version}",
    version = publishedArtifact.version,
    buildNumber = publishedArtifact.buildNumber,
    createdAt = publishedArtifact.createdAt,
    replacedAt = replacedAt,
    deployedAt = deployedAt,
    gitMetadata = if (publishedArtifact.gitMetadata == null) {
      null
    } else {
      publishedArtifact.gitMetadata?.toDgs()
    },
    environment = environmentName,
    reference = publishedArtifact.reference,
    status = MD_ArtifactStatusInEnvironment.valueOf(status.name),
    isCurrent = isCurrent
  )

