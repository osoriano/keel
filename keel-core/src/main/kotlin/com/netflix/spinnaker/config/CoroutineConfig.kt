package com.netflix.spinnaker.config

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

/**
 * Common coroutine configuration for all coroutine scopes.
 */
@Configuration
@EnableConfigurationProperties(CoroutineProperties::class)
class CoroutineConfig {
  @Bean
  fun coroutineExecutor(config: CoroutineProperties): ExecutorService =
    Executors.newFixedThreadPool(config.threadPoolSize)

  /**
   * Common coroutine context optimized for I/O, given the nature of most of the work performed by Keel.
   */
  @Bean
  fun workhorseCoroutineContext(config: CoroutineProperties, coroutineExecutor: ExecutorService): WorkhorseCoroutineContext =
    DefaultWorkhorseCoroutineContext
    // Example of using a custom thread pool as a dispatcher. In an experiment with threadPoolSize = 32 and
    // maxParallelism = 128, performance was slightly worse than with Dispatchers.IO
    // @OptIn(ExperimentalCoroutinesApi::class)
    // coroutineExecutor
    //  .asCoroutineDispatcher()
    //  .limitedParallelism(config.maxParallelism)
}
