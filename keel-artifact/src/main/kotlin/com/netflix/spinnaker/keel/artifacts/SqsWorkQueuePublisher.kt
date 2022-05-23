package com.netflix.spinnaker.keel.artifacts

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.MessageAttributeValue
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.config.WorkProcessingConfig
import com.netflix.spinnaker.keel.scm.CodeEvent
import org.springframework.stereotype.Component
import java.time.Clock

@Component
class SqsWorkQueuePublisher(
  private val sqsClient: AmazonSQS,
  private val objectMapper: ObjectMapper,
  private val clock: Clock,
  private val workProcessingConfig: WorkProcessingConfig
) : WorkQueuePublisher {

  override fun queueArtifactForProcessing(artifactVersion: PublishedArtifact) {
    sqsClient.sendMessage(
      SendMessageRequest(workProcessingConfig.artifactSqsQueueUrl, objectMapper.writeValueAsString(artifactVersion))
        .withMessageAttributes(
          mapOf("EnqueueTimestamp" to MessageAttributeValue().withStringValue(clock.instant().toString()).withDataType("String"))
        )
    )
  }

  override fun queueCodeEventForProcessing(codeEvent: CodeEvent) {
    sqsClient.sendMessage(
      SendMessageRequest(workProcessingConfig.codeEventSqsQueueUrl, objectMapper.writeValueAsString(codeEvent))
        .withMessageAttributes(
          mapOf("EnqueueTimestamp" to MessageAttributeValue().withStringValue(clock.instant().toString()).withDataType("String"))
        )
    )
  }
}
