package com.netflix.spinnaker.config

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asExecutor
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.SimpleApplicationEventMulticaster

@Configuration
class EventsConfiguration {
  @Bean
  fun applicationEventMulticaster() =
    SimpleApplicationEventMulticaster().apply {
      setTaskExecutor(Dispatchers.IO.asExecutor())
    }
}
