package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsDataLoader
import com.netflix.graphql.dgs.context.DgsContext
import com.netflix.spinnaker.keel.api.action.EnvironmentArtifactAndVersion
import com.netflix.spinnaker.keel.core.api.ArtifactVersionVetoData
import com.netflix.spinnaker.keel.graphql.types.MD_VersionVeto
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.springboot.scheduling.DefaultExecutor
import org.dataloader.BatchLoaderEnvironment
import org.dataloader.MappedBatchLoaderWithContext
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executor

/**
 * Loads all constraint states for the given versions
 */
@DgsDataLoader(name = VetoedDataLoader.Descriptor.name)
class VetoedDataLoader(
  private val keelRepository: KeelRepository,
  @DefaultExecutor private val executor: Executor
) : MappedBatchLoaderWithContext<EnvironmentArtifactAndVersion, MD_VersionVeto> {

  object Descriptor {
    const val name = "artifact-version-vetoed"
  }

  override fun load(keys: MutableSet<EnvironmentArtifactAndVersion>, environment: BatchLoaderEnvironment):
    CompletionStage<MutableMap<EnvironmentArtifactAndVersion, MD_VersionVeto>> {
    val context: ApplicationContext = DgsContext.getCustomContext(environment)
    return executor.supplyAsync {
      val results: MutableMap<EnvironmentArtifactAndVersion, MD_VersionVeto> = mutableMapOf()
      val vetoed = keelRepository.vetoedEnvironmentVersions(context.getConfig())

      vetoed.forEach { envArtifact ->
        envArtifact.versions.map { version ->
          results.put(
            EnvironmentArtifactAndVersion(environmentName = envArtifact.targetEnvironment, artifactReference = envArtifact.artifact.reference, artifactVersion = version.version),
            version.toDgs()
          )
        }
      }
      results
    }
  }
}

fun ArtifactVersionVetoData.toDgs() =
  MD_VersionVeto(vetoedBy = vetoedBy, vetoedAt = vetoedAt, comment = comment)
