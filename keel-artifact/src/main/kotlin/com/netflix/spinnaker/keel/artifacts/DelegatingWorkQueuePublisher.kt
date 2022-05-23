package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.scm.CodeEvent
import org.springframework.context.annotation.Primary
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component

@Primary
@Component
class DelegatingWorkQueuePublisher(
  private val sqsWorkQueuePublisher: SqsWorkQueuePublisher,
  private val sqlWorkQueuePublisher: SqlWorkQueuePublisher,
  private val springEnv: Environment
) : WorkQueuePublisher {

  override fun queueArtifactForProcessing(artifactVersion: PublishedArtifact) {
    if (springEnv.isSqsEnabled()) {
      sqsWorkQueuePublisher.queueArtifactForProcessing(artifactVersion)
    } else {
      sqlWorkQueuePublisher.queueArtifactForProcessing(artifactVersion)
    }
  }

  override fun queueCodeEventForProcessing(codeEvent: CodeEvent) {
    if (springEnv.isSqsEnabled()) {
      sqsWorkQueuePublisher.queueCodeEventForProcessing(codeEvent)
    } else {
      sqlWorkQueuePublisher.queueCodeEventForProcessing(codeEvent)
    }
  }

  private fun Environment.isSqsEnabled(): Boolean =
    springEnv.getProperty("keel.work-processing.sqs-enabled", Boolean::class.java, false)
}
