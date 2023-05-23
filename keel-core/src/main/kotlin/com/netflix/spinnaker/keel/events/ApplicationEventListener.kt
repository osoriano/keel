package com.netflix.spinnaker.keel.events

import com.netflix.spinnaker.keel.persistence.ResourceRepository
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch

@Component
class ApplicationEventListener(private val resourceRepository: ResourceRepository) {
  @EventListener(ApplicationEvent::class)
  fun onResourceEvent(event: ApplicationEvent) {
    GlobalScope.launch(Dispatchers.IO) {
      resourceRepository.appendHistory(event)
    }
  }
}
