package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsDataLoader
import com.netflix.graphql.dgs.context.DgsContext
import com.netflix.spinnaker.keel.graphql.types.MD_PinnedVersion
import com.netflix.spinnaker.keel.persistence.KeelRepository
import org.dataloader.BatchLoaderEnvironment
import org.dataloader.MappedBatchLoaderWithContext
import java.util.concurrent.CompletionStage
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.core.api.PinType
import com.netflix.spinnaker.keel.core.api.PinnedEnvironment
import java.util.concurrent.CompletableFuture

/**
 * Loads pinned artifact versions for all of the environments and map them to artifact and environment.
 * It uses a customContext that should be set up the request tree to grab the deliveryConfig
 */
@DgsDataLoader(name = PinnedVersionInEnvironmentDataLoader.Descriptor.name)
class PinnedVersionInEnvironmentDataLoader(
  private val keelRepository: KeelRepository
) : MappedBatchLoaderWithContext<PinnedArtifactAndEnvironment, MD_PinnedVersion> {

  object Descriptor {
    const val name = "pinned-versions-in-environment"
  }

  override fun load(keys: MutableSet<PinnedArtifactAndEnvironment>, environment: BatchLoaderEnvironment): CompletionStage<MutableMap<PinnedArtifactAndEnvironment, MD_PinnedVersion>> {
    val context: ApplicationContext = DgsContext.getCustomContext(environment)
    return CompletableFuture.supplyAsync {
      keelRepository.pinnedEnvironments(context.getConfig()).associateBy(
        { PinnedArtifactAndEnvironment(artifact = it.artifact, environment = it.targetEnvironment) },
        {
          val versionData = keelRepository.getArtifactVersion(artifact = it.artifact, version = it.version, status = null)
          it.toDgs(versionData)
        }
      ).toMutableMap()
    }
  }
}

fun PinnedEnvironment.toDgs(versionData: PublishedArtifact?) =
  MD_PinnedVersion(
    id = "$targetEnvironment-${artifact.reference}",
    name = artifact.name,
    reference = artifact.reference,
    version = version,
    pinnedAt = pinnedAt,
    pinnedBy = pinnedBy,
    comment = comment,
    buildNumber = versionData?.buildNumber,
    gitMetadata = versionData?.gitMetadata?.toDgs(),
    type = type?.toDgs()
  )



data class PinnedArtifactAndEnvironment(
  val artifact: DeliveryArtifact,
  val environment: String
)
