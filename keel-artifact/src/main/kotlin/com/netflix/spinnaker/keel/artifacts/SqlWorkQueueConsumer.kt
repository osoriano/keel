package com.netflix.spinnaker.keel.artifacts

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.config.DefaultWorkhorseCoroutineContext
import com.netflix.spinnaker.config.WorkhorseCoroutineContext
import com.netflix.spinnaker.keel.activation.DiscoveryActivated
import com.netflix.spinnaker.keel.config.WorkProcessingConfig
import com.netflix.spinnaker.keel.logging.blankMDC
import com.netflix.spinnaker.keel.persistence.WorkQueueRepository
import com.netflix.spinnaker.keel.telemetry.recordDuration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.springframework.context.ApplicationEventPublisher
import org.springframework.core.env.Environment
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock

@Component
class SqlWorkQueueConsumer(
  private val artifactQueueProcessor: ArtifactQueueProcessor,
  private val drainer: SqlWorkQueueDrainer,
  private val workQueueRepository: WorkQueueRepository,
  private val clock: Clock,
  private val spectator: Registry,
  private val springEnv: Environment,
  private val config: WorkProcessingConfig,
  private val publisher: ApplicationEventPublisher,
) : DiscoveryActivated(), CoroutineScope {

  override val coroutineContext: WorkhorseCoroutineContext = DefaultWorkhorseCoroutineContext

  private val artifactBatchSize: Int
    get() = springEnv.getProperty("keel.work-processing.artifact-batch-size", Int::class.java, config.artifactBatchSize)

  private val codeEventBatchSize: Int
    get() = springEnv.getProperty("keel.work-processing.code-event-batch-size", Int::class.java, config.codeEventBatchSize)

  companion object {
    private const val ARTIFACT_PROCESSING_DRIFT_GAUGE = "work.processing.artifact.drift"
    private const val CODE_EVENT_PROCESSING_DRIFT_GAUGE = "work.processing.code.drift"
  }

  @Scheduled(fixedDelayString = "\${keel.artifact-processing.frequency:PT1S}")
  fun consumeArtifactQueue() {
    if (shouldConsume()) {
      val startTime = clock.instant()
      val job = launch(blankMDC) {
        supervisorScope {
          workQueueRepository
            .removeArtifactsFromQueue(artifactBatchSize)
            .forEach { artifactVersion ->
              if (shouldDrain()) {
                drainer.drainToSqs(artifactVersion)
              } else {
                artifactQueueProcessor.processArtifact(artifactVersion)
              }
            }
        }
      }
      runBlocking { job.join() }
      spectator.recordDuration(ARTIFACT_PROCESSING_DRIFT_GAUGE, startTime, clock.instant())
    }
  }

  @Scheduled(fixedDelayString = "\${keel.artifact-processing.frequency:PT1S}")
  fun consumeCodeEventQueue() {
    if (shouldConsume()) {
      val startTime = clock.instant()
      val job = launch(blankMDC) {
        supervisorScope {
          workQueueRepository
            .removeCodeEventsFromQueue(codeEventBatchSize)
            .forEach { codeEvent ->
              // TODO(rz): Original code says the event is published here to throttle, but I don't see how that works
              //  in practice.
              publisher.publishEvent(codeEvent)
            }
        }
      }
      runBlocking { job.join() }
      spectator.recordDuration(CODE_EVENT_PROCESSING_DRIFT_GAUGE, startTime, clock.instant())
    }
  }

  private fun shouldConsume(): Boolean =
    enabled.get() && (!springEnv.getProperty("keel.work-processing.sqs-enabled", Boolean::class.java, false) || shouldDrain())

  private fun shouldDrain(): Boolean =
    springEnv.getProperty("keel.work-processing.draining-enabled", Boolean::class.java, false)
}
