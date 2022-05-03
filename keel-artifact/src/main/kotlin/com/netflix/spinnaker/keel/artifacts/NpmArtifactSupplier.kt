package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.config.DefaultWorkhorseCoroutineContext
import com.netflix.spinnaker.config.WorkhorseCoroutineContext
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.NPM
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier
import com.netflix.spinnaker.keel.api.plugins.SupportedArtifact
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.igor.artifact.ArtifactMetadataService
import com.netflix.spinnaker.keel.igor.artifact.ArtifactService
import kotlinx.coroutines.CoroutineScope
import org.springframework.stereotype.Component

/**
 * Built-in keel implementation of [ArtifactSupplier] for NPM artifacts.
 *
 * Note: this implementation currently makes some Netflix-specific assumptions with regards to artifact
 * versions so that it can extract build and commit metadata.
 */
@Component
class NpmArtifactSupplier(
  override val eventPublisher: EventPublisher,
  private val artifactService: ArtifactService,
  override val artifactMetadataService: ArtifactMetadataService,
  override val coroutineContext: WorkhorseCoroutineContext = DefaultWorkhorseCoroutineContext
) : BaseArtifactSupplier<NpmArtifact, NpmVersionSortingStrategy>(artifactMetadataService), CoroutineScope {

  override val supportedArtifact = SupportedArtifact(NPM, NpmArtifact::class.java)

  override suspend fun getLatestArtifact(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact): PublishedArtifact? =
    getLatestArtifacts(deliveryConfig, artifact, 1).firstOrNull()

  override suspend fun getLatestArtifacts(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    limit: Int
  ): List<PublishedArtifact> {
    return artifactService
      .getVersions(artifact.nameForQuery, NPM)
      // FIXME: this is making N calls to fill in data for each version so we can sort.
      //  Ideally, we'd make a single call to return the list with details for each version.
      .also {
        log.warn("About to make ${it.size} calls to artifact service to retrieve version details...")
      }
      .map { version ->
        artifactService.getArtifact(artifact.name, version, NPM)
      }
      .sortedWith(artifact.sortingStrategy.comparator)
      .take(limit) // versioning strategies return descending by default... ¯\_(ツ)_/¯
  }

  /**
   * Extracts a version display name from version string using the Netflix semver convention.
   */
  override fun getVersionDisplayName(artifact: PublishedArtifact): String {
    return NetflixVersions.getVersionDisplayName(artifact)
  }


  // The API requires colons in place of slashes to avoid path pattern conflicts
  private val DeliveryArtifact.nameForQuery: String
    get() = name.replace("/", ":")

  // Currently, we don't have any limitations for NPM artifact versions
  override fun shouldProcessArtifact(artifact: PublishedArtifact) = true
}
