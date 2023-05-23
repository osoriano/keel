package com.netflix.spinnaker.keel.lifecycle

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
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
    GlobalScope.launch(Dispatchers.IO) {
      val eventId = repository.saveEvent(event)
      if (event.startMonitoring) {
        publisher.publishEvent(StartMonitoringEvent(eventId, event))
      }
    }
  }
}
