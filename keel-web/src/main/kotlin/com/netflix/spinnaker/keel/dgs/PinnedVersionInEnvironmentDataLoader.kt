package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsDataLoader
import com.netflix.graphql.dgs.context.DgsContext
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.core.api.PinnedEnvironment
import com.netflix.spinnaker.keel.graphql.types.MD_PinnedVersion
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.springboot.scheduling.DefaultExecutor
import org.dataloader.BatchLoaderEnvironment
import org.dataloader.MappedBatchLoaderWithContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executor

/**
 * Loads pinned artifact versions for all of the environments and map them to artifact and environment.
 * It uses a customContext that should be set up the request tree to grab the deliveryConfig
 */
@DgsDataLoader(name = PinnedVersionInEnvironmentDataLoader.Descriptor.name)
class PinnedVersionInEnvironmentDataLoader(
  private val keelRepository: KeelRepository,
  @DefaultExecutor private val executor: Executor,
) : MappedBatchLoaderWithContext<PinnedArtifactAndEnvironment, MD_PinnedVersion> {

  val log: Logger by lazy { LoggerFactory.getLogger(javaClass) }

  object Descriptor {
    const val name = "pinned-versions-in-environment"
  }

  override fun load(keys: MutableSet<PinnedArtifactAndEnvironment>, environment: BatchLoaderEnvironment): CompletionStage<MutableMap<PinnedArtifactAndEnvironment, MD_PinnedVersion>> {
    val context: ApplicationContext = DgsContext.getCustomContext(environment)
    return executor.supplyAsync {
      keelRepository.pinnedEnvironments(context.getConfig()).associateBy(
        {
          PinnedArtifactAndEnvironment(artifactUniqueId = it.artifact.uniqueId(), environment = it.targetEnvironment)
        },
        {
          val versionData = keelRepository.getArtifactVersion(artifact = it.artifact, version = it.version)
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
