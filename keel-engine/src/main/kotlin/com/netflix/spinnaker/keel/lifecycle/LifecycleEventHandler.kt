package com.netflix.spinnaker.keel.lifecycle

import com.netflix.spinnaker.keel.persistence.LifecycleEventRepository
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

/**
 * Receives lifecycle events and stores them.
 */
@Component
class LifecycleEventHandler(
  val repository: LifecycleEventRepository,
  val publisher: ApplicationEventPublisher
) {

  /**
   * Saves all lifecycle events.
   * If they should be monitored, publish a [StartMonitoringEvent]
   */
  @EventListener(LifecycleEvent::class)
  fun handleEvent(event: LifecycleEvent) {
    val eventId = repository.saveEvent(event)
    if (event.startMonitoring) {
      publisher.publishEvent(StartMonitoringEvent(eventId, event))
    }
  }
}
