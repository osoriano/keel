package com.netflix.spinnaker.keel.bakery.artifact

import brave.Tracer
import com.netflix.frigga.ami.AppVersion
import com.netflix.spinnaker.keel.actuation.ArtifactHandler
import com.netflix.spinnaker.keel.api.actuation.Task
import com.netflix.spinnaker.keel.api.actuation.TaskLauncher
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.events.ArtifactRegisteredEvent
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.bakery.BaseImageCache
import com.netflix.spinnaker.keel.bakery.UnknownBaseImage
import com.netflix.spinnaker.keel.clouddriver.ImageService
import com.netflix.spinnaker.keel.clouddriver.getLatestNamedImages
import com.netflix.spinnaker.keel.clouddriver.model.baseImageName
import com.netflix.spinnaker.keel.exceptions.NoKnownArtifactVersions
import com.netflix.spinnaker.keel.igor.artifact.ArtifactService
import com.netflix.spinnaker.keel.lifecycle.LifecycleEvent
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventScope.PRE_DEPLOYMENT
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventStatus.NOT_STARTED
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventType.BAKE
import com.netflix.spinnaker.keel.logging.withCoroutineTracingContext
import com.netflix.spinnaker.keel.model.OrcaJob
import com.netflix.spinnaker.keel.parseAppVersion
import com.netflix.spinnaker.keel.persistence.BakedImageRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.NoSuchArtifactException
import com.netflix.spinnaker.keel.persistence.PausedRepository
import com.netflix.spinnaker.keel.telemetry.ArtifactCheckSkipped
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.core.env.Environment

/**
 * Responsible for launching bake tasks for Debian artifact versions that have not yet been baked into AMIs.
 */
class ImageHandler(
  private val repository: KeelRepository,
  private val baseImageCache: BaseImageCache,
  private val bakedImageRepository: BakedImageRepository,
  private val igorService: ArtifactService,
  private val imageService: ImageService,
  private val publisher: ApplicationEventPublisher,
  private val taskLauncher: TaskLauncher,
  private val defaultCredentials: BakeCredentials,
  private val pausedRepository: PausedRepository,
  private val springEnv: Environment,
  private val tracer: Tracer? = null
) : ArtifactHandler {

  override suspend fun handle(artifact: DeliveryArtifact) {
    if (artifact is DebianArtifact && !artifact.isPaused()) {
      val appVersions = try {
        artifact.findRelevantArtifactVersions()
      } catch (e: NoKnownArtifactVersions) {
        log.debug(e.message)
        return
      }

      val desiredBaseAmiName = try {
        artifact.findLatestBaseAmiName()
      } catch (e: UnknownBaseImage) {
        log.error("Unable to find the right base image for $appVersions for ${artifact.name}. " +
          "Is the base label '${artifact.vmOptions.baseLabel}' an actual valid base label?")
        return
      }

<<<<<<< 6d85bb7e6884ca89ff98194a542e311218f00811
        val images = artifact.findLatestAmi(desiredAppVersion)
        val imagesWithOlderBaseImages = images.filterValues { it.baseImageName != desiredBaseAmiName }
        val missingRegions = artifact.vmOptions.regions - images.keys
        when {
          images.isEmpty() -> {
            log.info("No AMI found for {}", desiredAppVersion)
            launchBake(artifact, desiredAppVersion)
          }
          imagesWithOlderBaseImages.isNotEmpty() && !artifact.vmOptions.ignoreBaseUpdates -> {
            log.info("AMIs for {} are outdated, rebaking…", desiredAppVersion)
            launchBake(
              artifact,
              desiredAppVersion,
              description = "Bake $desiredAppVersion due to a new base image: $desiredBaseAmiName"
            )
          }
          missingRegions.isNotEmpty() -> {
            log.warn("Detected missing regions for ${desiredAppVersion}: ${missingRegions.joinToString()}")
            publisher.publishEvent(ImageRegionMismatchDetected(desiredAppVersion, desiredBaseAmiName, images.keys, artifact.vmOptions.regions))
            launchBake(
              artifact,
              desiredAppVersion,
              regions = missingRegions,
              description = "Bake $desiredAppVersion due to missing regions: ${missingRegions.joinToString()}"
            )
          }
          else -> {
            log.debug("Image for {} already exists with app version {} and base image {} in regions {}", artifact.name, desiredAppVersion, desiredBaseAmiName, artifact.vmOptions.regions.joinToString())
=======
      appVersions.forEach { appVersion ->
        if (taskLauncher.correlatedTasksRunning(artifact.correlationId(appVersion))) {
          publisher.publishEvent(
            ArtifactCheckSkipped(artifact.type, artifact.name, "ActuationInProgress")
          )
        } else {
          if (!artifact.wasPreviouslyBakedWith(appVersion, desiredBaseAmiName)) {
            withCoroutineTracingContext(artifact, appVersion, tracer) {
              val images = artifact.findLatestAmi(appVersion)
              val imagesWithOlderBaseImages = images.filterValues { it.baseImageName != desiredBaseAmiName }
              val missingRegions = artifact.vmOptions.regions - images.keys
              when {
                images.isEmpty() -> {
                  log.info("No AMI found for {}", appVersion)
                  launchBake(artifact, appVersion)
                }
                imagesWithOlderBaseImages.isNotEmpty() -> {
                  log.info("AMIs for {} are outdated, rebaking…", appVersion)
                  launchBake(
                    artifact,
                    appVersion,
                    description = "Bake $appVersion due to a new base image: $desiredBaseAmiName"
                  )
                }
                missingRegions.isNotEmpty() -> {
                  log.warn("Detected missing regions for ${appVersion}: ${missingRegions.joinToString()}")
                  publisher.publishEvent(
                    ImageRegionMismatchDetected(appVersion, desiredBaseAmiName, images.keys, artifact.vmOptions.regions)
                  )
                  launchBake(
                    artifact,
                    appVersion,
                    regions = missingRegions,
                    description = "Bake $appVersion due to missing regions: ${missingRegions.joinToString()}"
                  )
                }
                else -> {
                  log.debug(
                    "Image for {} already exists with app version {} and base image {} in regions {}",
                    artifact.name,
                    appVersion,
                    desiredBaseAmiName,
                    artifact.vmOptions.regions.joinToString()
                  )
                }
              }
            }
>>>>>>> 52f634c96403b1408fcc549281c52577a0149697
          }
        }
      }
    }
  }

  private fun DeliveryArtifact.isPaused(): Boolean =
    if (deliveryConfigName == null) {
      false
    } else {
      val config = repository.getDeliveryConfig(deliveryConfigName!!)
      pausedRepository.applicationPaused(config.application)
    }

  private fun DebianArtifact.wasPreviouslyBakedWith(
    desiredAppVersion: String,
    desiredBaseAmiName: String
  ) =
    bakedImageRepository
      .getByArtifactVersion(desiredAppVersion, this)
      ?.let {
        it.baseAmiName == desiredBaseAmiName && it.amiIdsByRegion.keys.containsAll(vmOptions.regions)
      } ?: false

  private suspend fun DebianArtifact.findLatestAmi(desiredArtifactVersion: String) =
    imageService.getLatestNamedImages(
      appVersion = AppVersion.parseName(desiredArtifactVersion),
      account = defaultImageAccount,
      regions = vmOptions.regions,
      baseOs = vmOptions.baseOs
    )

  private val defaultImageAccount: String
    get() = springEnv.getProperty("images.default-account", String::class.java, "test")

  private fun DebianArtifact.findLatestBaseAmiName() =
    baseImageCache.getBaseAmiName(vmOptions.baseOs, vmOptions.baseLabel)

  /**
   * First checks our repo, and if a version isn't found checks igor.
   */
  private suspend fun DebianArtifact.findRelevantArtifactVersions(): Iterable<String> {
    try {
      val knownVersions = latestVersions() + repository.versionsInUse(this)
      if (knownVersions.isNotEmpty()) {
        return knownVersions
          .also { them ->
            log.debug("Known or in-use versions of $name = ${them.joinToString()}")
          }
      }
    } catch (e: NoSuchArtifactException) {
      log.debug("Latest known version of $name = null")
      if (!repository.isRegistered(name, type)) {
        // we clearly care about this artifact, let's register it.
        repository.register(this)
      }
    }

    // even though the artifact isn't registered we should grab the latest version to use
    val versions = igorService
      .getVersions(name, DEBIAN)
    log.debug("Finding latest version of $name: versions igor knows about = $versions")
    return versions
      .firstOrNull()
      ?.let {
        val version = "$name-$it"
        log.debug("Finding latest version of $name, choosing = $version")
        listOf(version)
      } ?: throw NoKnownArtifactVersions(this)
  }

  private fun DebianArtifact.latestVersions() =
    repository
      .artifactVersions(this, 1)
      .firstOrNull()
      ?.version
      ?.let(::setOf) ?: emptySet()

  private suspend fun launchBake(
    artifact: DebianArtifact,
    desiredVersion: String,
    regions: Set<String> = artifact.vmOptions.regions,
    description: String = "Bake $desiredVersion"
  ): List<Task> {
    // TODO: Frigga and Rocket version parsing are not aligned. We should consolidate.
    val appVersion = desiredVersion.parseAppVersion()
    val packageName = appVersion.packageName
    val version = desiredVersion.substringAfter("$packageName-")
    val fullArtifact = repository.getArtifactVersion(
      artifact,
      desiredVersion
    )
    val arch = fullArtifact?.metadata?.get("arch") ?: "all"
    val artifactRef = "/${packageName}_${version}_$arch.deb"
    val artifactPayload = mapOf(
      "type" to "DEB",
      "customKind" to false,
      "name" to artifact.name,
      "version" to version,
      "location" to "rocket",
      "reference" to artifactRef,
      "metadata" to emptyMap<String, Any>(),
      "provenance" to "n/a"
    )

    log.info("baking new image for {}", desiredVersion)

    val (serviceAccount, application) = artifact.taskAuthenticationDetails

    try {
      val taskRef = taskLauncher.submitJob(
        user = serviceAccount,
        application = application,
        environmentName = null,
        resourceId = null,
        notifications = emptySet(),
        description = description,
        correlationId = artifact.correlationId(desiredVersion),
        stages = listOf(
          OrcaJob(
            "bake",
            mapOf(
              "amiSuffix" to "",
              "baseOs" to artifact.vmOptions.baseOs,
              "baseLabel" to artifact.vmOptions.baseLabel.name.lowercase(),
              "cloudProviderType" to "aws",
              "package" to artifactRef.substringAfterLast("/"),
              "regions" to regions,
              "storeType" to artifact.vmOptions.storeType.name.lowercase(),
              "vmType" to "hvm"
            )
          )
        ),
        artifacts = listOf(artifactPayload),
      )
      publisher.publishEvent(BakeLaunched(desiredVersion))
      publisher.publishEvent(
        LifecycleEvent(
          scope = PRE_DEPLOYMENT,
          deliveryConfigName = checkNotNull(artifact.deliveryConfigName),
          artifactReference = artifact.reference,
          artifactVersion = desiredVersion,
          type = BAKE,
          id = "bake-$desiredVersion",
          status = NOT_STARTED,
          text = "Launching bake for $version",
          link = taskRef.id,
          startMonitoring = true
        )
      )
      return listOf(Task(id = taskRef.id, name = description))
    } catch (e: Exception) {
      log.error("Error launching bake for: $description")
      return emptyList()
    }
  }

  private val DebianArtifact.taskAuthenticationDetails: BakeCredentials
    get() = deliveryConfigName?.let {
      repository.getDeliveryConfig(it).run {
        BakeCredentials(serviceAccount, application)
      }
    } ?: defaultCredentials

  private val log by lazy { LoggerFactory.getLogger(javaClass) }
}

/**
 * Use the version in the correlation id so that we can bake for multiple versions at once
 */
internal fun DebianArtifact.correlationId(version: String): String =
  "bake:$name:$version"

data class BakeCredentials(
  val serviceAccount: String,
  val application: String
)

data class ImageRegionMismatchDetected(
  val appVersion: String,
  val baseAmiName: String,
  val foundRegions: Set<String>,
  val desiredRegions: Set<String>
)

data class RecurrentBakeDetected(val appVersion: String, val baseAmiVersion: String)
data class BakeLaunched(val appVersion: String)
