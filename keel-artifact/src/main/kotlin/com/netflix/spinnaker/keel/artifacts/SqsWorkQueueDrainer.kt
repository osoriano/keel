package com.netflix.spinnaker.keel.artifacts

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.persistence.WorkQueueRepository
import org.springframework.stereotype.Component

/**
 * Drains an SQS queue into SQL.
 */
@Component
class SqsWorkQueueDrainer(
  private val workQueueRepository: WorkQueueRepository,
  private val spectator: Registry
) {

  private val drainCounterId = spectator.createId("keel.work-processing.drained-items", "sink", "sql")

  suspend fun drainToSql(artifactVersion: PublishedArtifact) {
    try {
      workQueueRepository.addToQueue(artifactVersion)
      spectator.counter(drainCounterId.withTag("success", "true")).increment()
    } catch (e: Exception) {
      spectator.counter(drainCounterId.withTag("success", "false")).increment()
    }
  }
}
