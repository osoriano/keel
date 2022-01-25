package com.netflix.spinnaker.keel.artifacts

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter
import com.netflix.spinnaker.keel.activation.DiscoveryActivated
import com.netflix.spinnaker.keel.api.artifacts.ArtifactType
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.events.ArtifactVersionDetected
import com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier
import com.netflix.spinnaker.keel.api.plugins.supporting
import com.netflix.spinnaker.keel.config.WorkProcessingConfig
import com.netflix.spinnaker.keel.exceptions.InvalidSystemStateException
import com.netflix.spinnaker.keel.igor.BuildService
import com.netflix.spinnaker.keel.igor.model.BuildDetail
import com.netflix.spinnaker.keel.lifecycle.LifecycleEvent
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventScope
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventStatus
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventType
import com.netflix.spinnaker.keel.logging.blankMDC
import com.netflix.spinnaker.keel.logging.withThreadTracingContext
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.WorkQueueRepository
import com.netflix.spinnaker.keel.scm.CodeEvent
import com.netflix.spinnaker.keel.telemetry.ARTIFACT_DELAY
import com.netflix.spinnaker.keel.telemetry.recordDuration
import com.netflix.spinnaker.keel.telemetry.safeIncrement
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeout
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.core.env.Environment
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext

/**
 * A worker that processes queued artifacts while an instance is in service.
 *
 * Handles saving new artifacts to the queue, and reading from that queue and processing the artifacts.
 * Saves fully formed artifact versions to be used by DeliveryArtifacts
 */
@EnableConfigurationProperties(WorkProcessingConfig::class)
@Component
final class WorkQueueProcessor(
  private val config: WorkProcessingConfig,
  private val workQueueRepository: WorkQueueRepository,
  private val repository: KeelRepository,
  private val buildService: BuildService,
  private val artifactSuppliers: List<ArtifactSupplier<*, *>>,
  private val publisher: ApplicationEventPublisher,
  private val spectator: Registry,
  private val clock: Clock,
  private val springEnv: Environment,
  private val objectMapper: ObjectMapper
): DiscoveryActivated(), CoroutineScope {

  override val coroutineContext: CoroutineContext = Dispatchers.IO

  companion object {
    private const val ARTIFACT_PROCESSING_DRIFT_GAUGE = "work.processing.artifact.drift"
    private const val CODE_EVENT_PROCESSING_DRIFT_GAUGE = "work.processing.code.drift"
    private const val ARTIFACT_PROCESSING_DURATION = "work.processing.artifact.duration"
    private const val CODE_EVENT_PROCESSING_DURATION = "work.processing.code.duration"
    private const val ARTIFACT_UPDATED_COUNTER_ID = "keel.artifact.updated"
    private const val NUMBER_QUEUED_GAUGE = "work.processing.queued.number"
  }

  private val artifactBatchSize: Int
    get() = springEnv.getProperty("keel.work-processing.artifact-batch-size", Int::class.java, config.artifactBatchSize)

  private val codeEventBatchSize: Int
    get() = springEnv.getProperty("keel.work-processing.code-event-batch-size", Int::class.java, config.codeEventBatchSize)

  init {
    PolledMeter
      .using(spectator)
      .withName(NUMBER_QUEUED_GAUGE)
      .monitorValue(this) {
        when(enabled.get()) {
          true -> it.queueSize()
          false -> 0.0
        }
      }
  }

  private val lastArtifactCheck: AtomicReference<Instant> =
    createDriftGauge(ARTIFACT_PROCESSING_DRIFT_GAUGE)

  private val lastCodeCheck: AtomicReference<Instant> =
    createDriftGauge(CODE_EVENT_PROCESSING_DRIFT_GAUGE)

  private fun queueSize(): Double =
    workQueueRepository.queueSize().toDouble()

  fun queueArtifactForProcessing(artifactVersion: PublishedArtifact) {
    workQueueRepository.addToQueue(artifactVersion)
  }

  fun queueCodeEventForProcessing(codeEvent: CodeEvent) {
    workQueueRepository.addToQueue(codeEvent)
  }

  @Scheduled(fixedDelayString = "\${keel.artifact-processing.frequency:PT1S}")
  fun processArtifacts() {
    if (enabled.get()) {
      val startTime = clock.instant()
      val job = launch(blankMDC) {
        supervisorScope {
           workQueueRepository
              .removeArtifactsFromQueue(artifactBatchSize)
              .forEach { artifactVersion ->
                try {
                  /**
                   * Allow individual artifact processing to timeout but catch the `CancellationException`
                   * to prevent the cancellation of all coroutines under [job]
                   */
                  log.debug("Processing artifact {}", artifactVersion)
                  withTimeout(config.timeoutDuration.toMillis()) {
                    launch {
                      handlePublishedArtifact(artifactVersion)
                      lastArtifactCheck.set(clock.instant())
                    }
                  }
                } catch (e: TimeoutCancellationException) {
                  log.error("Timed out processing artifact version {}:", artifactVersion.version, e)
                }
              }
            }
        }
      runBlocking { job.join() }
      spectator.recordDuration(ARTIFACT_PROCESSING_DURATION, clock, startTime)
    }
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
          log.info("Registering version {} (status={}) of {} artifact {}",
            artifact.version, artifact.status, artifact.type, artifact.name)

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

  @Scheduled(fixedDelayString = "\${keel.artifact-processing.frequency:PT1S}")
  fun processCodeEvents() {
    if (enabled.get()) {
      val startTime = clock.instant()
      val job = launch(blankMDC) {
        supervisorScope {
          workQueueRepository
            .removeCodeEventsFromQueue(codeEventBatchSize)
            .forEach { codeEvent ->
              // publishing the event here throttles the influx of code events
              // so that we can deal with them at a slower pace
              publisher.publishEvent(codeEvent)
              lastCodeCheck.set(clock.instant())
            }
        }
      }
      runBlocking { job.join() }
      spectator.recordDuration(CODE_EVENT_PROCESSING_DURATION, clock, startTime)
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
  internal fun enrichAndStore(artifact: PublishedArtifact, supplier: ArtifactSupplier<*,*>): Boolean {
    val enrichedArtifact = supplier.addMetadata(artifact.normalized())
    notifyArtifactVersionDetected(enrichedArtifact)

    log.debug("Storing artifact ${artifact.type}:${artifact.name} version ${artifact.version}")
    val stored = repository.storeArtifactVersion(enrichedArtifact)

    if (stored && enrichedArtifact.createdAt != null) {
      with(enrichedArtifact) {
        // record how long it took us to store this version since the artifact was created
        log.debug("Recording storage delay for $type:$name: ${Duration.between(createdAt!!, clock.instant())}")
        spectator.recordDuration(ARTIFACT_DELAY, clock, createdAt!!,
          "delayType" to "storage",
          "artifactType" to type,
          "artifactName" to name
        )
      }
    }

    return stored
  }

  /**
   * Returns a copy of the [PublishedArtifact] with the git and build metadata populated, if available.
   */
  private fun ArtifactSupplier<*, *>.addMetadata(artifact: PublishedArtifact): PublishedArtifact {
    // only add metadata if either build or git metadata is null
    if (artifact.buildMetadata == null || artifact.gitMetadata == null) {
      val artifactMetadata = runBlocking {
        try {
          getArtifactMetadata(artifact)
        } catch (ex: Exception) {
          log.error("Could not fetch artifact metadata for name ${artifact.name} and version ${artifact.version}", ex)
          null
        }
      }
      return artifact.copy(gitMetadata = artifactMetadata?.gitMetadata, buildMetadata = artifactMetadata?.buildMetadata)
    }
    return artifact
  }

  /**
   * Finds the delivery configs that are using an artifact,
   * and publishes an [ArtifactVersionDetected] event and a build [LifecycleEvent] for them.
   */
  internal fun notifyArtifactVersionDetected(artifact: PublishedArtifact) {
    repository
      .getAllArtifacts(artifact.artifactType, artifact.name)
      .forEach { deliveryArtifact ->
        deliveryArtifact.deliveryConfigName?.let { configName ->
          val deliveryConfig = repository.getDeliveryConfig(configName)

          publisher.publishEvent(
            ArtifactVersionDetected(
              deliveryConfig = deliveryConfig,
              artifact = deliveryArtifact,
              version = artifact
            )
          )

          if (artifact.buildMetadata != null) {
            log.debug("Publishing build lifecycle event for published artifact $artifact")
            val data = mutableMapOf(
              "buildNumber" to artifact.buildNumber,
              "commitId" to artifact.commitHash,
              "buildMetadata" to artifact.buildMetadata,
              "application" to deliveryConfig.application
            )

            publisher.publishEvent(
              LifecycleEvent(
                scope = LifecycleEventScope.PRE_DEPLOYMENT,
                deliveryConfigName = configName,
                artifactReference = deliveryArtifact.reference,
                artifactVersion = artifact.version,
                type = LifecycleEventType.BUILD,
                id = "build-${artifact.version}",
                // the build has already started, and is maybe complete.
                // We use running to convey that to users, and allow the [BuildLifecycleMonitor] to immediately
                // update the status
                status = LifecycleEventStatus.RUNNING,
                text = "Monitoring build for ${artifact.version}",
                link = artifact.buildMetadata?.uid,
                data = data,
                timestamp = artifact.buildMetadata?.startedAtInstant,
                startMonitoring = true
              )
            )
          }
        }
      }
  }

  /**
   * Fetches the "image.properties" file from the Jenkins build disguised in this [PublishedArtifact], and fills in
   * the details of the Docker artifact it is intended to represent.
   *
   * The "image.properties" file is generated by Newt when publishing the Docker image in the build.
   */
  private fun PublishedArtifact.completeDockerImageDetails(): PublishedArtifact {
    val buildDetail = buildDetail
      ?: error("Cannot complete Docker image details. Build details missing or mal-formed: (metadata: $metadata)")

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
        "createdAt" to Instant.ofEpochMilli(buildDetail.completedAt)
      )
    )
  }

  private val PublishedArtifact.artifactType: ArtifactType
    get() = artifactTypeNames.find { it == type.lowercase() }
      ?.let { type.lowercase() }
      ?: throw InvalidSystemStateException("Unable to find registered artifact type for '$type'")

  private val PublishedArtifact.buildDetail: BuildDetail?
    get() = metadata["buildDetail"] ?.let { objectMapper.convertValue<BuildDetail>(it) }

  private val artifactTypeNames by lazy {
    artifactSuppliers.map { it.supportedArtifact.name }
  }

  private fun createDriftGauge(name: String): AtomicReference<Instant> =
    PolledMeter
      .using(spectator)
      .withName(name)
      .monitorValue(AtomicReference(clock.instant())) { previous ->
        when(enabled.get()) {
          true -> secondsSince(previous)
          false -> 0.0
        }
      }

  private fun secondsSince(start: AtomicReference<Instant>) : Double  =
    Duration
      .between(start.get(), clock.instant())
      .toMillis()
      .toDouble()
      .div(1000)
}
