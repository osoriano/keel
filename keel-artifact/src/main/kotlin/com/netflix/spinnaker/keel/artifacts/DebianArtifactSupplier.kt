package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.config.DefaultWorkhorseCoroutineContext
import com.netflix.spinnaker.config.WorkhorseCoroutineContext
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier
import com.netflix.spinnaker.keel.api.plugins.SupportedArtifact
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.igor.artifact.ArtifactMetadataService
import com.netflix.spinnaker.keel.igor.artifact.ArtifactService
import com.netflix.spinnaker.keel.parseAppVersionOrNull
import kotlinx.coroutines.CoroutineScope
import org.springframework.stereotype.Component

/**
 * Built-in keel implementation of [ArtifactSupplier] for Debian artifacts.
 *
 * Note: this implementation currently makes some Netflix-specific assumptions with regards to artifact
 * versions so that it can extract build and commit metadata.
 */
@Component
class DebianArtifactSupplier(
  override val eventPublisher: EventPublisher,
  private val artifactService: ArtifactService,
  override val artifactMetadataService: ArtifactMetadataService,
  override val coroutineContext: WorkhorseCoroutineContext = DefaultWorkhorseCoroutineContext
) : BaseArtifactSupplier<DebianArtifact, DebianVersionSortingStrategy>(artifactMetadataService), CoroutineScope {
  override val supportedArtifact = SupportedArtifact("deb", DebianArtifact::class.java)

  override suspend fun getLatestVersion(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact): PublishedArtifact? =
    getLatestVersions(deliveryConfig, artifact, 1).firstOrNull()

  override suspend fun getLatestVersions(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    limit: Int
  ): List<PublishedArtifact> {
    log.info("Fetching latest $limit debian versions for $artifact")
    val versions = artifactService.getVersions(artifact.name, DEBIAN)

    val importantVersions = versions
      // FIXME: this is making N calls to fill in data for each version so we can sort.
      //  Ideally, we'd make a single call to return the list with details for each version.
      .also {
        log.warn("About to make ${it.size} calls to artifact service to retrieve version details...")
      }
      .map { version ->
        artifactService.getArtifact(artifact.name, version, DEBIAN)
      }
      .sortedWith(artifact.sortingStrategy.comparator)
      .take(limit) // versioning strategies return descending by default... ¯\_(ツ)_/¯

    return importantVersions.map {
      // add the correct architecture to the metadata
      //  given a reference like debian-local:pool/w/my-deb/my-deb_1.0.0~rc.1-h5.23b8241_all.deb
      //  pull out the arch (the 'all' before the '.deb') and add that to the metadata
      val arch = it.reference.split("_").lastOrNull()?.split(".")?.firstOrNull()
      if (arch != null) {
        it.copy(metadata = it.metadata + mapOf("arch" to arch))
      } else {
        it
      }
    }
  }

  override fun getVersionDisplayName(artifact: PublishedArtifact): String {
    // TODO: Frigga and Rocket version parsing are not aligned. We should consolidate.
    val appversion = artifact.version.parseAppVersionOrNull()
    if (appversion != null) {
      return if (appversion.version != null) {
        appversion.version
      } else {
        artifact.version.removePrefix("${artifact.name}-")
      }
    }
    return artifact.version
  }

  override fun shouldProcessArtifact(artifact: PublishedArtifact): Boolean =
    artifact.hasWellFormedVersion()


  // Debian Artifacts should contain a releaseStatus in the metadata
  private fun PublishedArtifact.hasReleaseStatus() : Boolean {
    return if (this.metadata.containsKey("releaseStatus") && this.metadata["releaseStatus"] != null) {
      true
    } else {
      log.debug("Ignoring artifact event without release status: $this")
      false
    }
  }

  // Debian Artifacts should not have "local" as a part of their version string
  private fun PublishedArtifact.hasWellFormedVersion() : Boolean {
    val appversion = "${this.name}-${this.version}".parseAppVersionOrNull()
    return if (appversion != null && appversion.buildNumber != null) {
      if (appversion.buildNumber.contains("local")) {
        log.debug("Ignoring artifact which contains local is its version string: $this")
        false
      } else {
        //appversion is not null, and the version does not contain "local"
        true
      }
    } else {
      log.debug("Either appversion or appversion.buildNumber is null. Ignoring this version")
      false
    }
  }
}
