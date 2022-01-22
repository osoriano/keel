package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.keel.api.artifacts.ArtifactMetadata
import com.netflix.spinnaker.keel.api.events.ArtifactPublishedEvent
import com.netflix.spinnaker.keel.api.events.ArtifactSyncEvent
import com.netflix.spinnaker.keel.api.plugins.UnsupportedArtifactException
import com.netflix.spinnaker.keel.artifacts.WorkQueueProcessor
import com.netflix.spinnaker.keel.artifacts.isArtifactEvent
import com.netflix.spinnaker.keel.artifacts.isBuildEvent
import com.netflix.spinnaker.keel.artifacts.isIncompleteDockerArtifact
import com.netflix.spinnaker.keel.igor.artifact.ArtifactMetadataService
import com.netflix.spinnaker.keel.logging.withThreadTracingContext
import com.netflix.spinnaker.keel.scm.isCodeEvent
import com.netflix.spinnaker.keel.scm.toCodeEvent
import com.netflix.spinnaker.keel.yaml.APPLICATION_YAML_VALUE
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.http.HttpStatus.ACCEPTED
import org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping(path = ["/artifacts"])
class ArtifactController(
  private val eventPublisher: ApplicationEventPublisher,
  private val artifactMetadataService: ArtifactMetadataService,
  private val workQueueProcessor: WorkQueueProcessor,
  private val springEnv: ConfigurableEnvironment
) {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  private val optimizedDockerFlow: Boolean
    get() = springEnv.getProperty("keel.artifacts.optimized-docker-flow", Boolean::class.java, true)

  /**
   * Handles "artifact events" submitted from other Spinnaker services (Echo for Debian and NPM packages,
   * Igor for Docker images).
   *
   * TODO: At Netflix, the events originating from Echo are essentially relayed from our CI service, Rocket,
   *  and include events that are not necessarily about artifacts, such as SCM events like "commit created",
   *  "PR merged", etc. Currently, we take advantage of this fact to enable use cases that require monitoring
   *  code repos, such as preview environments. This should be refactored in the future to use a separate and
   *  proper API for code events.
   */
  @PostMapping(
    path = ["/events"],
    consumes = [APPLICATION_JSON_VALUE]
  )
  @ResponseStatus(ACCEPTED)
  fun submitArtifact(@RequestBody echoArtifactEvent: EchoArtifactEvent) {
    log.debug("Received artifact event: $echoArtifactEvent")
    echoArtifactEvent.payload.artifacts.forEach { artifact ->
      when {
        artifact.isCodeEvent -> {
          artifact.toCodeEvent()?.let { codeEvent ->
            log.debug("Queueing code event: $codeEvent")
            workQueueProcessor.queueCodeEventForProcessing(codeEvent)
          }
        }
        /** For Docker, we may receive incomplete artifact events from Echo originating from Rocket BUILD events.
         *  In this case, we still queue the artifact and [WorkQueueProcessor] will fill in the details at the
         *  time of processing. */
        artifact.isIncompleteDockerArtifact -> {
          if (optimizedDockerFlow) {
            workQueueProcessor.queueArtifactForProcessing(artifact)
          } else {
            log.debug("Ignoring incomplete docker artifact as feature toggle is off: $artifact")
          }
        }
        // It's not a build event from Rocket, so it must be either an artifact event from Rocket or from Clouddriver/Igor (for Docker)
        !artifact.isBuildEvent -> {
          withThreadTracingContext(artifact) {
            try {
              log.debug("Queueing artifact ${artifact.type}:${artifact.name} version ${artifact.version} from artifact $artifact")
              workQueueProcessor.queueArtifactForProcessing(artifact)
            } catch (e: UnsupportedArtifactException) {
              log.debug("Ignoring artifact event with unsupported type {}: {}", artifact.type, artifact)
            }
          }
        }
        else -> log.debug("Ignoring unsupported artifact event: $artifact")
      }
    }
  }

  @PostMapping(
    path = ["/sync"]
  )
  @ResponseStatus(ACCEPTED)
  fun sync() {
    eventPublisher.publishEvent(ArtifactSyncEvent(true))
  }

  // This endpoint is calling Igor (and then the CI provider) under the covers.
  @GetMapping(
    path = ["/build/{buildNumber}/commit/{commitId}"],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  fun getArtifactMetadata(
    @PathVariable buildNumber: String,
    @PathVariable commitId: String
  ): ArtifactMetadata? =
     try {
       runBlocking {
         artifactMetadataService.getArtifactMetadata(buildNumber, commitId)
       }
    } catch (ex: Exception) {
      if(buildNumber == "LOCAL") {
        // this is expected as metadata service doesn't provide info for local builds
        log.info("tried to get artifact metadata for LOCAL build, commit $commitId", ex)
      } else {
        log.error("failed to get artifact metadata for build $buildNumber and commit $commitId", ex)
      }
      null
    }
}

data class EchoArtifactEvent(
  val payload: ArtifactPublishedEvent,
  val eventName: String
)
