package com.netflix.spinnaker.keel.persistence

import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.lifecycle.LifecycleEvent
import com.netflix.spinnaker.keel.lifecycle.LifecycleStep

interface LifecycleEventRepository {

  /**
   * Adds event to list of events
   *
   * @return the uid of the saved event
   */
  fun saveEvent(event: LifecycleEvent): String

  /**
   * Returns all raw events for artifact versions
   */
  fun getEvents(artifact: DeliveryArtifact, artifactVersions: List<String>): List<LifecycleEvent>

  /**
   * Returns the event summaries by type ("steps") for artifact versions
   */
  fun getSteps(artifact: DeliveryArtifact, artifactVersions: List<String>): List<LifecycleStep>

  /**
   * Returns the event summaries by type ("steps") for all known artifact versions
   */
  fun getSteps(artifact: DeliveryArtifact): List<LifecycleStep>
}
