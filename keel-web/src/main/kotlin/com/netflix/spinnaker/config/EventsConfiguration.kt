package com.netflix.spinnaker.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.SimpleApplicationEventMulticaster
import org.springframework.core.task.TaskExecutor
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor

@Configuration
class EventsConfiguration {
  @Bean
  fun applicationEventTaskExecutor(@Value("\${keel.events.pool-size:10}") poolSize: Int) =
    ThreadPoolTaskExecutor().apply {
      corePoolSize = poolSize
      threadNamePrefix = "event-pool-"
    }

  @Bean
  fun applicationEventMulticaster(applicationEventTaskExecutor: TaskExecutor) =
    SimpleApplicationEventMulticaster().apply {
      setTaskExecutor(applicationEventTaskExecutor)
    }
}
