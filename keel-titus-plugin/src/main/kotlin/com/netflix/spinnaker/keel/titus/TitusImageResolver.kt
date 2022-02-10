package com.netflix.spinnaker.keel.titus

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.docker.ContainerProvider
import com.netflix.spinnaker.keel.docker.DockerImageResolver
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.titus.exceptions.ImageTooOld
import com.netflix.spinnaker.keel.titus.exceptions.NoDigestFound
import com.netflix.spinnaker.keel.titus.registry.TitusRegistryService
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration

/**
 * Assumption: docker container digest is the same in all regions
 */
@Component
class TitusImageResolver(
  override val repository: KeelRepository,
  private val clock: Clock,
  private val cloudDriverService: CloudDriverService,
  private val cloudDriverCache: CloudDriverCache,
  private val titusRegistryService: TitusRegistryService,
  override val featureToggles: FeatureToggles
) : DockerImageResolver<TitusClusterSpec>(
  repository,
  featureToggles
) {
  override val supportedKind = TITUS_CLUSTER_V1

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  override fun getContainerFromSpec(resource: Resource<TitusClusterSpec>) =
    resource.spec.container

  override fun getAccountFromSpec(resource: Resource<TitusClusterSpec>) =
    resource.spec.locations.account

  override fun updateContainerInSpec(
    resource: Resource<TitusClusterSpec>,
    container: ContainerProvider,
    artifact: DockerArtifact,
    tag: String
  ) =
    resource.copy(
      spec = resource.spec.copy(
        container = container,
        _artifactName = artifact.name,
        artifactVersion = tag
      )
    )

  override fun getTags(account: String, organization: String, image: String) =
    runBlocking {
      val repository = "$organization/$image"
      cloudDriverService.findDockerTagsForImage(account, repository)
    }

  override fun getDigest(titusAccount: String, artifact: DockerArtifact, tag: String) =
    runBlocking {
      val images = titusRegistryService.findImages(artifact.name, titusAccount, tag)
      val digest = images.firstOrNull()?.digest

      if (digest == null) {
        val publishedArtifact = repository.getArtifactVersion(artifact, tag)
        if (publishedArtifact?.createdAt?.isBefore(clock.instant() - TITUS_REGISTRY_IMAGE_TTL) == true) {
          throw ImageTooOld(artifact.name, tag, publishedArtifact.createdAt!!)
        } else {
          val registry = cloudDriverCache.getRegistryForTitusAccount(titusAccount)
          throw NoDigestFound(artifact.name, tag, registry) // sha should be the same in all accounts for titus
        }
      }

      digest
    }

  companion object {
    val TITUS_REGISTRY_IMAGE_TTL: Duration = Duration.ofDays(60)
  }
}
