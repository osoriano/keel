package com.netflix.spinnaker.keel.artifacts

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest
import com.amazonaws.services.sqs.model.ReceiveMessageRequest
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter
import com.netflix.spectator.impl.AtomicDouble
import com.netflix.spinnaker.config.DefaultWorkhorseCoroutineContext
import com.netflix.spinnaker.config.WorkhorseCoroutineContext
import com.netflix.spinnaker.keel.activation.DiscoveryActivated
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.config.WorkProcessingConfig
import com.netflix.spinnaker.keel.logging.blankMDC
import com.netflix.spinnaker.keel.scm.CodeEvent
import com.netflix.spinnaker.keel.telemetry.recordDuration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.springframework.context.ApplicationEventPublisher
import org.springframework.core.env.Environment
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Instant
import kotlin.math.min

@Component
class SqsWorkQueueConsumer(
  private val artifactQueueProcessor: ArtifactQueueProcessor,
  private val drainer: SqsWorkQueueDrainer,
  private val clock: Clock,
  private val spectator: Registry,
  private val springEnv: Environment,
  private val config: WorkProcessingConfig,
  private val publisher: ApplicationEventPublisher,
  private val sqsClient: AmazonSQS,
  private val objectMapper: ObjectMapper,
) : DiscoveryActivated(), CoroutineScope {

  override val coroutineContext: WorkhorseCoroutineContext = DefaultWorkhorseCoroutineContext

  private val artifactBatchSize: Int
    get() = min(springEnv.getProperty("keel.work-processing.artifact-batch-size", Int::class.java, config.artifactBatchSize), 10)

  private val codeEventBatchSize: Int
    get() = min(springEnv.getProperty("keel.work-processing.code-event-batch-size", Int::class.java, config.codeEventBatchSize), 10)

  private var artifactQueueDepth = AtomicDouble(-1.0)
  private var codeEventQueueDepth = AtomicDouble(-1.0)

  private val queueDepthMeter = PolledMeter.poll(spectator) {
    artifactQueueDepth.get().also {
      if (it >= 0.0) {
        spectator.gauge(ARTIFACT_QUEUE_DEPTH).set(it)
      }
    }
    codeEventQueueDepth.get().also {
      if (it >= 0.0) {
        spectator.gauge(CODE_EVENT_QUEUE_DEPTH).set(it)
      }
    }
  }

  companion object {
    private const val ARTIFACT_PROCESSING_TIME = "work.processing.artifact.duration"
    private const val CODE_EVENT_PROCESSING_TIME = "work.processing.code.duration"

    private const val ARTIFACT_PROCESSING_DELAY = "work.processing.artifact.delay"
    private const val CODE_EVENT_PROCESSING_DELAY = "work.processing.code.delay"

    private const val ARTIFACT_ITEMS_POLLED = "work.processing.artifact.items-polled"
    private const val CODE_EVENT_ITEMS_POLLED = "work.processing.code.items-polled"

    private const val ARTIFACT_QUEUE_DEPTH = "work.processing.artifact.queue-depth"
    private const val CODE_EVENT_QUEUE_DEPTH = "work.processing.code.queue-depth"
  }

  @Scheduled(fixedDelayString = "\${keel.artifact-processing.frequency:PT1S}")
  fun consumeArtifactQueue() {
    if (!shouldConsume()) {
      return
    }

    val result = sqsClient.receiveMessage(
      ReceiveMessageRequest(config.artifactSqsQueueUrl)
        .withMaxNumberOfMessages(artifactBatchSize)
        .withWaitTimeSeconds(20)
    )

    val job = launch(blankMDC) {
      supervisorScope {
        var itemsSuccess = 0L
        var itemsFailed = 0L
        result.messages.forEach { message ->
          val startTime = clock.instant()

          try {
            val artifact = objectMapper.readValue<PublishedArtifact>(message.body)
            if (shouldDrain()) {
              drainer.drainToSql(artifact)
            } else {
              artifactQueueProcessor.processArtifact(artifact)
            }
            itemsSuccess++
          } catch (e: Exception) {
            log.error("Failed to process artifact", e)
            itemsFailed++
            return@forEach
          }

          sqsClient.deleteMessage(config.artifactSqsQueueUrl, message.receiptHandle)
          spectator.recordDuration(ARTIFACT_PROCESSING_TIME, startTime, clock.instant())

          message.attributes["EnqueueTimestamp"]
            ?.let { Instant.parse(it) }
            ?.let { spectator.recordDuration(ARTIFACT_PROCESSING_DELAY, it, clock.instant()) }
            ?: log.warn("EnqueueTimestamp attribute not present on artifact message")
        }
        spectator.counter(ARTIFACT_ITEMS_POLLED, "success", "true").increment(itemsSuccess)
        spectator.counter(ARTIFACT_ITEMS_POLLED, "success", "false").increment(itemsFailed)
      }
    }
    runBlocking { job.join() }
  }

  @Scheduled(fixedDelayString = "\${keel.artifact-processing.frequency:PT1S}")
  fun consumeCodeEventQueue() {
    if (!shouldConsume()) {
      return
    }

    val result = sqsClient.receiveMessage(
      ReceiveMessageRequest(config.codeEventSqsQueueUrl)
        .withMaxNumberOfMessages(codeEventBatchSize)
        .withWaitTimeSeconds(20)
    )

    val job = launch(blankMDC) {
      supervisorScope {
        var itemsSuccess = 0L
        var itemsFailed = 0L
        result.messages.forEach { message ->
          val startTime = clock.instant()

          try {
            publisher.publishEvent(objectMapper.readValue<CodeEvent>(message.body))
            itemsSuccess++
          } catch (e: Exception) {
            log.error("Failed to process code event", e)
            itemsFailed++
            return@forEach
          }

          sqsClient.deleteMessage(config.codeEventSqsQueueUrl, message.receiptHandle)
          spectator.recordDuration(CODE_EVENT_PROCESSING_TIME, startTime, clock.instant())

          message.attributes["EnqueueTimestamp"]
            ?.let { Instant.parse(it) }
            ?.let { spectator.recordDuration(CODE_EVENT_PROCESSING_DELAY, it, clock.instant()) }
            ?: log.warn("EnqueueTimestamp attribute not present on code event message")
        }
        spectator.counter(CODE_EVENT_ITEMS_POLLED, "success", "true").increment(itemsSuccess)
        spectator.counter(CODE_EVENT_ITEMS_POLLED, "success", "false").increment(itemsFailed)
      }
    }
    runBlocking { job.join() }
  }

  @Scheduled(fixedRate = 60_000)
  fun monitorQueueDepth() {
    artifactQueueDepth.set(
      sqsClient.getQueueAttributes(
        GetQueueAttributesRequest(config.artifactSqsQueueUrl)
          .withAttributeNames("ApproximateNumberOfMessages")
      ).let {
        it.attributes["ApproximateNumberOfMessages"]?.toDouble() ?: -1.0
      }
    )
    codeEventQueueDepth.set(
      sqsClient.getQueueAttributes(
        GetQueueAttributesRequest(config.codeEventSqsQueueUrl)
          .withAttributeNames("ApproximateNumberOfMessages")
      ).let {
        it.attributes["ApproximateNumberOfMessages"]?.toDouble() ?: -1.0
      }
    )
  }

  private fun shouldConsume(): Boolean =
    !springEnv.activeProfiles.contains("laptop") &&
      enabled.get() &&
      (springEnv.getProperty("keel.work-processing.sqs-enabled", Boolean::class.java, false) || shouldDrain())

  private fun shouldDrain(): Boolean =
    springEnv.getProperty("keel.work-processing.draining-enabled", Boolean::class.java, false)
}
