package com.netflix.spinnaker.keel.events

import com.netflix.spinnaker.keel.persistence.ResourceRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class ResourceHistoryListener(
  private val resourceRepository: ResourceRepository
) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  @EventListener(ResourceEvent::class)
  fun onResourceEvent(event: ResourceEvent) {
    GlobalScope.launch(Dispatchers.IO) {
      try {
        resourceRepository.appendHistory(event)
      } catch (e: Exception) {
        log.error("Error handling resource event", e)
      }
    }
  }
}
