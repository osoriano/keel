package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.scm.CodeEvent

/**
 * Publish artifacts and code events to a work queue.
 */
interface WorkQueuePublisher {

  fun queueArtifactForProcessing(artifactVersion: PublishedArtifact)

  fun queueCodeEventForProcessing(codeEvent: CodeEvent)
}
