package com.netflix.spinnaker.keel.artifacts

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.artifacts.ArtifactType
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.copyOrCreate
import com.netflix.spinnaker.keel.api.events.ArtifactVersionStored
import com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier
import com.netflix.spinnaker.keel.api.plugins.supporting
import com.netflix.spinnaker.keel.config.WorkProcessingConfig
import com.netflix.spinnaker.keel.exceptions.InvalidSystemStateException
import com.netflix.spinnaker.keel.igor.BuildService
import com.netflix.spinnaker.keel.igor.model.BuildDetail
import com.netflix.spinnaker.keel.igor.model.TriggerEvent
import com.netflix.spinnaker.keel.lifecycle.LifecycleEvent
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventScope
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventScope.*
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventStatus
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventStatus.*
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventType
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventType.*
import com.netflix.spinnaker.keel.logging.withThreadTracingContext
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.telemetry.recordDuration
import com.netflix.spinnaker.keel.telemetry.safeIncrement
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Instant
import java.util.Properties

/**
 * A worker that processes queued artifacts while an instance is in service.
 *
 * Saves fully formed artifact versions to be used by DeliveryArtifacts
 */
@Component
class ArtifactQueueProcessor(
  private val config: WorkProcessingConfig,
  private val repository: KeelRepository,
  private val buildService: BuildService,
  private val artifactSuppliers: List<ArtifactSupplier<*, *>>,
  private val spectator: Registry,
  private val clock: Clock,
  private val publisher: ApplicationEventPublisher,
  private val objectMapper: ObjectMapper,
) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  companion object {
    private const val ARTIFACT_PROCESSING_DURATION = "work.processing.artifact.duration"
    private const val ARTIFACT_UPDATED_COUNTER_ID = "keel.artifact.updated"
  }

  suspend fun processArtifact(artifactVersion: PublishedArtifact) {
    val startTime = clock.instant()
    try {
      // Allow individual artifact processing to timeout but catch the `CancellationException`
      // to prevent the cancellation of all coroutines under [job]
      log.debug("Processing artifact {}", artifactVersion)
      withTimeout(config.timeoutDuration.toMillis()) {
        launch {
          handlePublishedArtifact(artifactVersion)
        }
      }
    } catch (e: TimeoutCancellationException) {
      log.error("Timed out processing artifact version {}:", artifactVersion.version, e)
    }
    spectator.recordDuration(ARTIFACT_PROCESSING_DURATION, startTime, clock.instant())
  }

  /**
   * Processes a new artifact version by enriching it with git/build metadata, and storing
   * it in the database.
   *
   * This method also acts as an event handler, allowing other components in the system to
   * trigger storage of artifact versions in flows that don't/can't use the work queue.
   */
  @EventListener(PublishedArtifact::class)
  fun handlePublishedArtifact(publishedArtifact: PublishedArtifact) {
    val artifact = try {
      publishedArtifact.let {
        if (it.isIncompleteDockerArtifact) {
          it.completeDockerImageDetails()
        } else {
          it
        }
      }
    } catch (e: Exception) {
      log.debug("Failed to complete Docker artifact: $publishedArtifact", e)
      return
    }

    withThreadTracingContext(artifact) {
      if (repository.isRegistered(artifact.name, artifact.artifactType)) {
        val artifactSupplier = artifactSuppliers.supporting(artifact.artifactType)
        if (artifactSupplier.shouldProcessArtifact(artifact)) {
          log.info(
            "Registering version {} (status={}) of {} artifact {}",
            artifact.version, artifact.status, artifact.type, artifact.name
          )

          enrichAndStore(artifact, artifactSupplier)
            .also { wasAdded ->
              if (wasAdded) {
                incrementUpdatedCount(artifact)
              }
            }
        } else {
          log.debug("Artifact $artifact shouldn't be processed due to supplier limitations. Ignoring this artifact version.")
        }
      } else {
        log.debug("Artifact ${artifact.type}:${artifact.name} is not registered. Ignoring new artifact version: $artifact")
      }
    }
  }

  private fun incrementUpdatedCount(artifact: PublishedArtifact) {
    spectator.counter(
      ARTIFACT_UPDATED_COUNTER_ID,
      listOf(
        BasicTag("artifactName", artifact.name),
        BasicTag("artifactType", artifact.type)
      )
    ).safeIncrement()
  }

  /**
   * Normalizes an artifact by calling [PublishedArtifact.normalized],
   * enriches it by adding git and build metadata,
   * creates the appropriate build lifecycle event,
   * and stores in the database
   */
  internal fun enrichAndStore(artifact: PublishedArtifact, supplier: ArtifactSupplier<*, *>): Boolean {
    log.debug("Storing artifact (before adding metadata from rocket) ${artifact.type}:${artifact.name} version ${artifact.version} with branch ${artifact.branch}")

    val artifactWithBasicInfo = artifact.copy(
      gitMetadata = artifact.gitMetadata.copyOrCreate(
        branch = artifact.branch,
        commit = artifact.commitHash
      ),
      buildMetadata = artifact.buildMetadata.copyOrCreate(
        number = artifact.buildNumber
      )
    ).normalized()

    // store a copy of an artifact, prior of enriching it with more metadata
    val stored = repository.storeArtifactVersion(artifactWithBasicInfo)

    if (stored) {
      publisher.publishEvent(ArtifactVersionStored(artifactWithBasicInfo))
    }

    val response = supplier.addMetadata(artifactWithBasicInfo)
    // if we were able to add metadata, update the artifact in DB
    if (response.metadataAdded) {
      repository.storeArtifactVersion(response.updatedArtifact)
    }
    // publish a build lifecycle event with using the most updated artifact
    findArtifactsAndPublishEvent(response.updatedArtifact)

    return stored
  }

  private val artifactTypeNames by lazy {
    artifactSuppliers.map { it.supportedArtifact.name }
  }

  private val PublishedArtifact.artifactType: ArtifactType
    get() = artifactTypeNames.find { it == type.lowercase() }
      ?.let { type.lowercase() }
      ?: throw InvalidSystemStateException("Unable to find registered artifact type for '$type'")

  private val PublishedArtifact.buildDetail: BuildDetail?
    get() = metadata["buildDetail"]?.let { objectMapper.convertValue<BuildDetail>(it) }

  private val PublishedArtifact.buildTriggerEvent: TriggerEvent?
    get() = metadata["triggerEvent"]?.let { objectMapper.convertValue<TriggerEvent>(it) }

  /**
   * Fetches the "image.properties" file from the Jenkins build disguised in this [PublishedArtifact], and fills in
   * the details of the Docker artifact it is intended to represent.
   *
   * The "image.properties" file is generated by Newt when publishing the Docker image in the build.
   */
  private fun PublishedArtifact.completeDockerImageDetails(): PublishedArtifact {
    val buildDetail = buildDetail
      ?: error("Cannot complete Docker image details. Build details missing or mal-formed: (metadata: $metadata)")

    val buildTriggerEvent = buildTriggerEvent
      ?: error("Cannot complete Docker image details. Build trigger event missing or mal-formed: (metadata: $metadata)")

    val imagePropsFile = buildDetail.artifacts.find { artifact ->
      artifact.endsWith("/image.properties") || artifact.endsWith("/image-server.properties")
    }
      ?.let { it.substringAfter("${buildDetail.buildUrl}/artifact/") }
      ?: error("Cannot complete Docker image details. No image properties file found in build artifacts (metadata: $metadata)")

    log.debug("Found Docker image properties file in build ${buildDetail.buildDisplayName}: $imagePropsFile")
    val imageDetails = runBlocking {
      buildService.getArtifactContents(
        controller = buildController ?: error("Missing build controller name in artifact metadata: $metadata"),
        job = buildJob ?: error("Missing build job name in artifact metadata: $metadata"),
        buildNumber = buildDetail.buildNumber,
        filePath = imagePropsFile
      ).let {
        Properties().apply { load(it.contents.inputStream()) }
      }
    }

    log.debug("Loaded Docker image properties from ${buildDetail.buildUrl}/$imagePropsFile")
    val imageName = imageDetails.getProperty("imageName", null)
      ?: error("Invalid format for Docker image properties file: $imageDetails")
    val (image, tag) = imageName.split(":")
    log.debug("Found Docker image from build event: $imageName. Build: ${buildDetail.buildUrl}")

    return copy(
      type = artifactType,
      name = image.trim(),
      version = tag.trim(),
      reference = buildDetail.buildUrl,
      // add just enough metadata to allow the ArtifactSupplier.addMetadata function above to do its thing
      metadata = metadata + mapOf(
        "buildNumber" to buildDetail.buildNumber.toString(),
        "commitId" to buildDetail.commitId,
        "createdAt" to Instant.ofEpochMilli(buildDetail.completedAt),
        "branch" to buildTriggerEvent.target.branchName
      )
    )
  }

  /**
   * Returning a copy of the [PublishedArtifact] with the git and build metadata populated, if available.
   */
  fun ArtifactSupplier<*, *>.addMetadata(artifact: PublishedArtifact): MetadataAndArtifact {
    return if (artifact.hasIncompleteMetadata()) {
      // only add metadata if either build or git metadata are currently missing information
      runBlocking {
        try {
          val artifactMetadata = getArtifactMetadata(artifact)
          val updatedArtifact = artifact.copy(
            gitMetadata = artifactMetadata?.gitMetadata ?: artifact.gitMetadata,
            buildMetadata = artifactMetadata?.buildMetadata ?: artifact.buildMetadata
          )
          log.debug("Storing updated artifact (after adding metadata from rocket): $updatedArtifact")
          return@runBlocking MetadataAndArtifact(true, updatedArtifact)
        } catch (ex: Exception) {
          // in case of an error from boost, don't override the current information
          log.debug("Could not fetch artifact metadata for name ${artifact.name} and version ${artifact.version}; using default data", ex)
          return@runBlocking MetadataAndArtifact(false, artifact)
        }
      }
    } else {
      return MetadataAndArtifact(false, artifact)
    }
  }

  private fun PublishedArtifact.hasIncompleteMetadata() =
    (buildMetadata == null || buildMetadata!!.incompleteMetadata()) || (gitMetadata == null || gitMetadata!!.incompleteMetadata())

  /**
   * Finds the delivery configs that are using an artifact,
   * and publishes a build [LifecycleEvent] for them.
   */
  internal fun findArtifactsAndPublishEvent(artifact: PublishedArtifact) {
    repository
      .getAllArtifacts(artifact.artifactType, artifact.name)
      .forEach { deliveryArtifact ->
        deliveryArtifact.deliveryConfigName?.let { configName ->
          val deliveryConfig = repository.getDeliveryConfig(configName)
          publishLifecycleEvent(artifact, deliveryConfig, deliveryArtifact)
        }
      }
  }

  private fun publishLifecycleEvent(artifact: PublishedArtifact, deliveryConfig: DeliveryConfig, deliveryArtifact: DeliveryArtifact) {
    log.debug("Publishing build lifecycle event for published artifact $artifact")
    val data = mutableMapOf(
      "buildNumber" to artifact.buildNumber,
      "commitId" to artifact.commitHash,
      "buildMetadata" to artifact.buildMetadata,
      "application" to deliveryConfig.application,
      "branch" to artifact.branch
    )

    publisher.publishEvent(
      LifecycleEvent(
        scope = PRE_DEPLOYMENT,
        deliveryConfigName = deliveryConfig.name,
        artifactReference = deliveryArtifact.reference,
        artifactVersion = artifact.version,
        type = BUILD,
        id = "build-${artifact.version}",
        // the build has already started, and is maybe complete.
        // We use running to convey that to users, and allow the [BuildLifecycleMonitor] to immediately
        // update the status
        status = RUNNING,
        text = "Monitoring build for ${artifact.version}",
        link = artifact.buildMetadata?.uid ?: "N/A",
        data = data,
        timestamp = artifact.buildMetadata?.startedAtInstant ?: Instant.now(),
        startMonitoring = true
      )
    )
  }

  /* store information if metadata was added to an artifact, and the updated artifact*/
  data class MetadataAndArtifact (
    val metadataAdded: Boolean,
    val updatedArtifact: PublishedArtifact
  )
}
