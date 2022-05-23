package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.persistence.WorkQueueRepository
import com.netflix.spinnaker.keel.scm.CodeEvent
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component

@Component
class SqlWorkQueuePublisher(
  private val workQueueRepository: WorkQueueRepository
) : WorkQueuePublisher {

  override fun queueArtifactForProcessing(artifactVersion: PublishedArtifact) {
    workQueueRepository.addToQueue(artifactVersion)
  }

  override fun queueCodeEventForProcessing(codeEvent: CodeEvent) {
    workQueueRepository.addToQueue(codeEvent)
  }
}
