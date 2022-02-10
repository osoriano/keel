package com.netflix.spinnaker.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@ConfigurationProperties("keel.scheduler")
class CheckSchedulerProperties {
  var threadPoolSize: Int = Runtime.getRuntime().availableProcessors()
  var coroutineThreadPoolSize: Int = Runtime.getRuntime().availableProcessors()
  var coroutineMaxParallelism: Int = coroutineThreadPoolSize * 8
}

@Configuration
@EnableConfigurationProperties(CheckSchedulerProperties::class)
class CheckSchedulerConfig {
  @Bean
  fun coroutineExecutor(config: CheckSchedulerProperties): ExecutorService =
    Executors.newFixedThreadPool(config.coroutineThreadPoolSize)
}
