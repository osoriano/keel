package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.context.DgsContext
import com.netflix.graphql.dgs.exceptions.DgsEntityNotFoundException
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Locatable
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.bakery.BakeryMetadataService
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.clouddriver.model.NamedImage
import com.netflix.spinnaker.keel.core.api.DEFAULT_SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.core.api.PromotionStatus
import com.netflix.spinnaker.keel.core.api.PublishedArtifactInEnvironment
import com.netflix.spinnaker.keel.graphql.types.MD_ArtifactVersionInEnvironment
import com.netflix.spinnaker.keel.graphql.types.MD_PackageDiff
import com.netflix.spinnaker.keel.retrofit.isNotFound
import com.netflix.spinnaker.kork.exceptions.SystemException
import graphql.schema.DataFetchingEnvironment
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import retrofit2.HttpException

/**
 * Support methods for [ApplicationFetcher].
 */
@Component
class ApplicationFetcherSupport(
  private val cloudDriverService: CloudDriverService,
  private val bakeryMetadataService: BakeryMetadataService?
  ) {

  companion object {
    private val log by lazy { LoggerFactory.getLogger(ApplicationFetcherSupport::class.java) }
  }

  /**
   * @return the [DeliveryConfig] associated with the DGS context.
   */
  fun getDeliveryConfigFromContext(dfe: DataFetchingEnvironment): DeliveryConfig {
    val context: ApplicationContext = DgsContext.getCustomContext(dfe)
    return context.getConfig()
  }

  /**
   * @return An [MD_PackageDiff] of the Debian packages between the current artifact version contained in the
   * DGS context, and the previous version.
   */
  fun getDebianPackageDiff(
    dfe: DataFetchingEnvironment
  ): MD_PackageDiff? {

    if (bakeryMetadataService == null) {
      return null
    }

    val diffContext = getDiffContext(dfe)

    with(diffContext) {
      val region = deliveryConfig.resourcesUsing(deliveryArtifact.reference, fetchedVersion.environmentName!!)
        .firstOrNull { it.spec is Locatable<*> }
        ?.let { (it.spec as Locatable<*>).locations.regions.first().name }
        ?: run {
          log.warn("Unable to determine region for $deliveryArtifact in environment ${fetchedVersion.environmentName}")
          return null
        }

      val fetchedImage = getNamedImage(fetchedVersion, region)
      val previousImage = previousDeployedVersion?.let { getNamedImage(it, region) }

      val diff = runBlocking {
        bakeryMetadataService.getPackageDiff(
          oldImage = previousImage?.imageName,
          newImage = fetchedImage.imageName,
          region = region
        )
      }

      return diff.toDgs()
    }
  }

  /**
   * @return the [NamedImage] from CloudDriver matching the [PublishedArtifactInEnvironment].
   */
  private fun getNamedImage(artifact: PublishedArtifactInEnvironment, region: String): NamedImage =
    runBlocking {
      val imageName = artifact.publishedArtifact.normalizedVersion
      try {
        cloudDriverService.namedImages(
          user = DEFAULT_SERVICE_ACCOUNT,
          imageName = imageName,
          account = null,
          region = region
        ).firstOrNull() ?: throw ImageNotFound(imageName)
      } catch (e: HttpException) {
        when (e.isNotFound) {
          true -> throw ImageNotFound(imageName)
          else -> throw e
        }
      }
    }

  /**
   * @return an [ArtifactDiffContext] object containing the [DeliveryConfig] and [DeliveryArtifact] associated
   * with the DGS context, along with a [ArtifactDiffContext.fetchedVersion] representing
   * the artifact version in the context, a [ArtifactDiffContext.previousDeployedVersion] for the previous version, and
   * [ArtifactDiffContext.currentDeployedVersion] for the currently deployed version, if applicable.
   */
  fun getDiffContext(
    dfe: DataFetchingEnvironment
  ): ArtifactDiffContext {
    val mdArtifactVersion: MD_ArtifactVersionInEnvironment = dfe.getLocalContext()
    val deliveryConfig = getDeliveryConfigFromContext(dfe)
    val applicationContext: ApplicationContext = DgsContext.getCustomContext(dfe) // the artifact versions store context

    val deliveryArtifact = deliveryConfig.matchingArtifactByReference(mdArtifactVersion.reference)
      ?: throw DgsEntityNotFoundException("Artifact ${mdArtifactVersion.reference} was not found in the delivery config") // the delivery artifact of this artifact

    val artifactVersions = mdArtifactVersion.environment?.let { applicationContext.getArtifactVersions(deliveryArtifact = deliveryArtifact, environmentName = it) }
      ?: throw DgsEntityNotFoundException("Environment ${mdArtifactVersion.environment} has not versions for artifact ${mdArtifactVersion.reference}")

    // The version we're currently fetching data for
    val fetchedVersion = artifactVersions.firstOrNull { it.publishedArtifact.version == mdArtifactVersion.version }
      ?: throw DgsEntityNotFoundException("artifact ${mdArtifactVersion.reference} has no version named ${mdArtifactVersion.version}")

    // The version that is currently deployed in this environment (if any)
    val currentDeployedVersion = artifactVersions.firstOrNull { it.status == PromotionStatus.CURRENT }

    // The version that was deployed before the fetchedVersion (if any)
    val previousDeployedVersion = artifactVersions.firstOrNull { it.replacedBy == fetchedVersion.publishedArtifact.version }

    return ArtifactDiffContext(deliveryConfig, deliveryArtifact, fetchedVersion, currentDeployedVersion, previousDeployedVersion)
  }
}

class ImageNotFound(imageName: String) : SystemException("Image $imageName not found")

data class ArtifactDiffContext(
  val deliveryConfig: DeliveryConfig,
  val deliveryArtifact: DeliveryArtifact,
  val fetchedVersion: PublishedArtifactInEnvironment,
  val currentDeployedVersion: PublishedArtifactInEnvironment?,
  val previousDeployedVersion: PublishedArtifactInEnvironment?
)
