package com.netflix.spinnaker.keel.events

import com.netflix.spinnaker.keel.persistence.ResourceRepository
import com.netflix.spinnaker.keel.services.ResourceStatusService
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Propagation.REQUIRED
import org.springframework.transaction.annotation.Transactional

@Component
class ResourceHistoryListener(
  private val resourceRepository: ResourceRepository,
  private val resourceStatusService: ResourceStatusService
) {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  @EventListener(ResourceEvent::class)
  @Transactional(propagation = REQUIRED)
  fun onResourceEvent(event: ResourceEvent) {
    // we first append the new event to history
    val previousEvent = resourceRepository.lastEvent(event.ref)
    resourceRepository.appendHistory(event)

    // and then update the resource status based on the short history of the last two events
    val shortHistory = listOfNotNull(event, previousEvent)
    val status = resourceStatusService.getStatusFromHistory(shortHistory)
    log.debug("Updating status of resource ${event.ref} to $status")
    resourceRepository.updateStatus(event.ref, status)
  }
}