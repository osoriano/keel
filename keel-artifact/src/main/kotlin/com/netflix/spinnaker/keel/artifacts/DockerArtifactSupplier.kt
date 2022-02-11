package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Locatable
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.BRANCH_JOB_COMMIT_BY_JOB
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.SEMVER_JOB_COMMIT_BY_JOB
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.SEMVER_JOB_COMMIT_BY_SEMVER
import com.netflix.spinnaker.keel.api.artifacts.SortingStrategy
import com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier
import com.netflix.spinnaker.keel.api.plugins.SupportedArtifact
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.igor.artifact.ArtifactMetadataService
import com.netflix.spinnaker.keel.titus.registry.TitusRegistryService
import org.springframework.stereotype.Component

/**
 * Built-in keel implementation of [ArtifactSupplier] that does not itself receive/retrieve artifact information
 * but is used by keel's `POST /artifacts/events` API to notify the core of new Docker artifacts.
 */
@Component
class DockerArtifactSupplier(
  override val eventPublisher: EventPublisher,
  override val artifactMetadataService: ArtifactMetadataService,
  private val titusRegistryService: TitusRegistryService,
) : BaseArtifactSupplier<DockerArtifact, DockerVersionSortingStrategy>(artifactMetadataService) {
  override val supportedArtifact = SupportedArtifact("docker", DockerArtifact::class.java)

<<<<<<< 9ac83e61819e44a21ac7b7d1e2c5b00e4250a8b0
<<<<<<< 4c76b4ad2323773a3a73e2741e3695005f0c66c0
<<<<<<< 9a74071d2401ab3654fed604310d90eb11dd94ff
  private fun findArtifactVersions(artifact: DeliveryArtifact, version: String? = null): List<PublishedArtifact> {
    return runWithIoContext {
      // TODO: we currently don't have a way to derive account information from artifacts,
      //  so we look in all accounts.
      cloudDriverService.findDockerImages(account = "*", repository = artifact.name, tag = version, includeDetails = true)
        .map { dockerImage ->
          PublishedArtifact(
            name = dockerImage.repository,
            type = DOCKER,
            reference = dockerImage.repository.substringAfter(':', dockerImage.repository),
            version = dockerImage.tag,
            metadata = let {
              val metadata = mutableMapOf<String, Any?>(
                "fullImagePath" to dockerImage.artifact?.reference,
                "clouddriverAccount" to dockerImage.account,
                "registry" to dockerImage.registry,
              )
              if (dockerImage.commitId != null && dockerImage.buildNumber != null) {
                metadata.putAll(mapOf(
                  "commitId" to dockerImage.commitId,
                  "prCommitId" to dockerImage.prCommitId,
                  "buildNumber" to dockerImage.buildNumber,
                  "branch" to dockerImage.branch,
                  "createdAt" to dockerImage.date
                ))
              }
              metadata
            }
          )
        }
=======
  private fun findArtifactVersions(artifact: DeliveryArtifact, limit: Int): List<PublishedArtifact> {
    // TODO: we currently don't have a way to derive account information from artifacts,
<<<<<<< b3859ed7eccc2ab980465dbaacc4d37c0a030215
    //  so we look in all accounts/registries.
    return if (featureToggles.isEnabled(OPTIMIZED_DOCKER_FLOW)) {
      titusRegistryService.findImages(artifact.name)
    } else {
      runWithIoContext {
        cloudDriverService.findDockerImages(registry = "*", repository = artifact.name, includeDetails = true)
      }
>>>>>>> d3fd8a09b7e726420d9315438a7435aa9e2a346f
    }
      .map { dockerImage ->
        PublishedArtifact(
          name = dockerImage.repository,
          type = DOCKER,
          reference = dockerImage.repository.substringAfter(':', dockerImage.repository),
          version = dockerImage.tag,
          metadata = let {
            if (dockerImage.commitId != null && dockerImage.buildNumber != null) {
              mapOf(
                "commitId" to dockerImage.commitId,
                "prCommitId" to dockerImage.prCommitId,
                "buildNumber" to dockerImage.buildNumber,
                "branch" to dockerImage.branch,
                "createdAt" to dockerImage.date
              )
            } else {
              emptyMap()
            }
=======
    //  so we search by image name only, i.e. we look in all accounts/registries.
=======
  private fun findArtifactVersions(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, limit: Int): List<PublishedArtifact> {
=======
  private suspend fun findArtifactVersions(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, limit: Int): List<PublishedArtifact> {
>>>>>>> 77a38e89aaf841a05240af60a0db0d0ad1aa3c91
    // TODO: We currently search by image name only, i.e. we look in all accounts/registries and regions, but
    //  we could use the info about clusters using the artifacts to narrow down.
>>>>>>> c449b3aefa1ace8696be7eab7682911f67e3ad94
    val images = titusRegistryService.findImages(artifact.name)
    return images.map { dockerImage ->
      PublishedArtifact(
        name = dockerImage.repository,
        type = DOCKER,
        reference = dockerImage.repository.substringAfter(':', dockerImage.repository),
        version = dockerImage.tag,
        metadata = let {
          if (dockerImage.commitId != null && dockerImage.buildNumber != null) {
            mapOf(
              "commitId" to dockerImage.commitId,
              "prCommitId" to dockerImage.prCommitId,
              "buildNumber" to dockerImage.buildNumber,
              "branch" to dockerImage.branch,
              "createdAt" to dockerImage.date
            )
          } else {
            emptyMap()
>>>>>>> ce8ad4d9b3c8a1056ae78ebb313e9d2dbee2719c
          }
        }
      )
    }
  }

  override suspend fun getLatestArtifact(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact): PublishedArtifact? =
    getLatestArtifacts(deliveryConfig, artifact, 1).firstOrNull()

  override suspend fun getLatestArtifacts(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    limit: Int
  ): List<PublishedArtifact> {
    if (artifact !is DockerArtifact) {
      throw IllegalArgumentException("Only Docker artifacts are supported by this implementation.")
    }

    return findArtifactVersions(deliveryConfig, artifact, limit)
      .filter { shouldProcessArtifact(it) }
      .sortedWith(artifact.sortingStrategy.comparator)
      .take(limit) // unfortunately we can only limit here because we need to sort with the comparator above
  }

  override fun parseDefaultBuildMetadata(artifact: PublishedArtifact, sortingStrategy: SortingStrategy): BuildMetadata? {
      if (sortingStrategy.hasBuild()) {
        val regex = Regex("""^.*-h(\d+).*$""")
        val result = regex.find(artifact.version)
        if (result != null && result.groupValues.size == 2) {
          return BuildMetadata(id = result.groupValues[1].toInt())
        }
      }
    return null
  }

  override fun parseDefaultGitMetadata(artifact: PublishedArtifact, sortingStrategy: SortingStrategy): GitMetadata? {
      if (sortingStrategy.hasCommit()) {
        return GitMetadata(commit = artifact.version.substringAfterLast("."))
      }
    return null
  }

  private fun SortingStrategy.hasBuild(): Boolean {
    return (this as? DockerVersionSortingStrategy)
      ?.let { it.strategy in listOf(BRANCH_JOB_COMMIT_BY_JOB, SEMVER_JOB_COMMIT_BY_JOB, SEMVER_JOB_COMMIT_BY_SEMVER) }
      ?: false
  }

  private fun SortingStrategy.hasCommit(): Boolean {
    return (this as? DockerVersionSortingStrategy)
      ?.let { it.strategy in listOf(BRANCH_JOB_COMMIT_BY_JOB, SEMVER_JOB_COMMIT_BY_JOB, SEMVER_JOB_COMMIT_BY_SEMVER) }
      ?: false
  }

  override fun shouldProcessArtifact(artifact: PublishedArtifact): Boolean =
    artifact.version != "latest"

}
