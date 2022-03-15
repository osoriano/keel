package com.netflix.spinnaker.keel.titus

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.plugins.Resolver
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.docker.ContainerProvider
import com.netflix.spinnaker.keel.docker.DigestProvider
import com.netflix.spinnaker.keel.docker.MultiReferenceContainerProvider
import com.netflix.spinnaker.keel.docker.ReferenceProvider
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.NoMatchingArtifactException
import com.netflix.spinnaker.keel.resolvers.DesiredVersionResolver
import com.netflix.spinnaker.keel.titus.exceptions.ImageTooOld
import com.netflix.spinnaker.keel.titus.exceptions.NoDigestFound
import com.netflix.spinnaker.keel.titus.registry.TitusRegistryService
import com.netflix.spinnaker.kork.exceptions.ConfigurationException
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
  val repository: KeelRepository,
  private val clock: Clock,
  private val cloudDriverService: CloudDriverService,
  private val cloudDriverCache: CloudDriverCache,
  private val titusRegistryService: TitusRegistryService,
  val desiredVersionResolver: DesiredVersionResolver
) : Resolver<TitusClusterSpec> {
  override val supportedKind = TITUS_CLUSTER_V1

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  override fun invoke(resource: Resource<TitusClusterSpec>): Resource<TitusClusterSpec> {
    val container = resource.spec.container
    if (container is DigestProvider || (container is MultiReferenceContainerProvider && container.references.isEmpty())) {
      return resource
    }

    val deliveryConfig = repository.deliveryConfigFor(resource.id)
    val environment = repository.environmentFor(resource.id)
    val account = resource.spec.locations.account

    val containers = mutableListOf<ContainerProvider>()
    if (container is MultiReferenceContainerProvider) {
      container.references.forEach {
        containers.add(ReferenceProvider(it))
      }
    } else {
      containers.add(container)
    }

    var updatedResource = resource
    containers.forEach {
      val artifact = getArtifact(it, deliveryConfig)
      val tag: String = desiredVersionResolver.getDesiredVersion(deliveryConfig, environment, artifact)

      val newContainer = getContainer(account, artifact, tag)
      updatedResource = resource.copy(
        spec = resource.spec.copy(
          container = newContainer,
          _artifactName = artifact.name,
          artifactVersion = tag
        )
      )
    }
    return updatedResource
  }

  fun getArtifact(container: ContainerProvider, deliveryConfig: DeliveryConfig): DockerArtifact =
    when (container) {
      is ReferenceProvider -> {
        deliveryConfig.artifacts.find { it.reference == container.reference && it.type == DOCKER } as DockerArtifact?
          ?: throw NoMatchingArtifactException(reference = container.reference, type = DOCKER, deliveryConfigName = deliveryConfig.name)
      }
      else -> throw ConfigurationException("Unsupported container provider ${container.javaClass}")
    }

  fun getContainer(
    account: String,
    artifact: DockerArtifact,
    tag: String
  ): DigestProvider {
    val digest = getDigest(account, artifact, tag)
    return DigestProvider(
      organization = artifact.organization,
      image = artifact.image,
      digest = digest
    )
  }

  fun getTags(account: String, organization: String, image: String) =
    runBlocking {
      val repository = "$organization/$image"
      cloudDriverService.findDockerTagsForImage(account, repository)
    }

  fun getDigest(titusAccount: String, artifact: DockerArtifact, tag: String): String {
      val images = runBlocking {
        titusRegistryService.findImages(artifact.name, titusAccount, tag)
      }
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

      return digest
    }

  companion object {
    val TITUS_REGISTRY_IMAGE_TTL: Duration = Duration.ofDays(60)
  }
}
