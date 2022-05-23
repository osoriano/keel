package com.netflix.spinnaker.keel.artifacts

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import org.springframework.stereotype.Component

/**
 * Drains a SQL queue into SQS.
 */
@Component
class SqlWorkQueueDrainer(
  private val spectator: Registry,
  private val sqsPublisher: SqsWorkQueuePublisher,
) {

  private val drainCounterId = spectator.createId("keel.work-processing.drained-items", "sink", "sqs")

  suspend fun drainToSqs(artifactVersion: PublishedArtifact) {
    try {
      sqsPublisher.queueArtifactForProcessing(artifactVersion)
      spectator.counter(drainCounterId.withTag("success", "true")).increment()
    } catch (e: Exception) {
      spectator.counter(drainCounterId.withTag("success", "false")).increment()
    }
  }
}
