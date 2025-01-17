package com.netflix.spinnaker.keel.dgs

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.graphql.dgs.DgsDataLoader
import com.netflix.graphql.dgs.context.DgsContext
import com.netflix.spinnaker.keel.graphql.types.MdPinnedVersion
import com.netflix.spinnaker.keel.persistence.KeelRepository
import org.dataloader.BatchLoaderEnvironment
import org.dataloader.MappedBatchLoaderWithContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletionStage
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.core.api.PinnedEnvironment
import java.util.concurrent.CompletableFuture

/**
 * Loads pinned artifact versions for all of the environments and map them to artifact and environment.
 * It uses a customContext that should be set up the request tree to grab the deliveryConfig
 */
@DgsDataLoader(name = PinnedVersionInEnvironmentDataLoader.Descriptor.name)
class PinnedVersionInEnvironmentDataLoader(
  private val keelRepository: KeelRepository
) : MappedBatchLoaderWithContext<PinnedArtifactAndEnvironment, MdPinnedVersion> {

  val log: Logger by lazy { LoggerFactory.getLogger(javaClass) }

  object Descriptor {
    const val name = "pinned-versions-in-environment"
  }

  override fun load(keys: MutableSet<PinnedArtifactAndEnvironment>, environment: BatchLoaderEnvironment): CompletionStage<MutableMap<PinnedArtifactAndEnvironment, MdPinnedVersion>> {
    val context: ApplicationContext = DgsContext.getCustomContext(environment)
    return CompletableFuture.supplyAsync {
      keelRepository.pinnedEnvironments(context.getConfig()).associateBy(
        {
          PinnedArtifactAndEnvironment(artifactUniqueId = it.artifact.uniqueId(), environment = it.targetEnvironment)
        },
        {
          val versionData = keelRepository.getArtifactVersion(artifact = it.artifact, version = it.version, status = null)
          it.toDgs(versionData)
        }
      ).toMutableMap()
    }
  }
}

fun PinnedEnvironment.toDgs(versionData: PublishedArtifact?) =
  MdPinnedVersion(
    id = "$targetEnvironment-${artifact.reference}",
    name = artifact.name,
    reference = artifact.reference,
    version = version,
    pinnedAt = pinnedAt,
    pinnedBy = pinnedBy,
    comment = comment,
    buildNumber = versionData?.buildNumber,
    gitMetadata = versionData?.gitMetadata?.toDgs()
  )

data class PinnedArtifactAndEnvironment(
  val artifactUniqueId: String,
  val environment: String
)

/**
 * Generates a unique id for an artifact to be used as a key for the data loader.
 * If we just use the whole DeliveryArtifact object then we run the risk of the key not working
 * if we add more data to the artifact (like, if we add extra info to the metadata in some calls but not others).
 *
 * In practice, keys should not be full objects.
 */
fun DeliveryArtifact.uniqueId(): String =
  "$deliveryConfigName:$type:$name:$reference"
