package com.netflix.spinnaker.config

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.asCoroutineDispatcher
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@ConfigurationProperties("keel.coroutines")
class CoroutineProperties {
  var threadPoolSize: Int = Runtime.getRuntime().availableProcessors()
  var maxParallelism: Int = threadPoolSize * 8
}

@Configuration
@EnableConfigurationProperties(CoroutineProperties::class)
class CoroutineConfig {
  @Bean
  fun coroutineExecutor(config: CoroutineProperties): ExecutorService =
    Executors.newFixedThreadPool(config.threadPoolSize)

  @Bean
  fun coroutineDispatcher(config: CoroutineProperties, coroutineExecutor: ExecutorService) =
    @OptIn(ExperimentalCoroutinesApi::class)
    coroutineExecutor
      .asCoroutineDispatcher()
      .limitedParallelism(config.maxParallelism)
}
