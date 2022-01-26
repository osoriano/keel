package com.netflix.spinnaker.keel.okhttp

import com.netflix.spinnaker.config.ServiceEndpoint
import com.netflix.spinnaker.config.okhttp3.OkHttpClientBuilderProvider
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor

/**
 * An [OkHttpClient] provider that returns clients instrumented for proper propagation of MDC context
 * to the underlying thread pool.
 */
class MdcAwareOkhttpClientProvider(
  private val clientProviders: List<OkHttpClientBuilderProvider>,
  private val threadPoolCoreSize: Int = 0,
  private val threadPoolMaxSize: Int = Int.MAX_VALUE
) {
  internal val dispatcher = ThreadPoolTaskExecutor().apply {
    corePoolSize = threadPoolCoreSize
    maxPoolSize = threadPoolMaxSize
    keepAliveSeconds = 60
    threadNamePrefix = "Okhttp MDC-aware dispatcher"
    setTaskDecorator(MdcTaskDecorator())
    initialize()
  }.let {
    Dispatcher(it.threadPoolExecutor)
  }

  /**
   * @return An [OkHttpClient] for the specified [endpoint] that is instrumented for proper propagation
   * of MDC context to the underlying thread pool.
   */
  fun getClient(endpoint: ServiceEndpoint): OkHttpClient {
    return clientProviders.find { it.supports(endpoint) }
      ?.get(endpoint)
      ?.dispatcher(dispatcher)
      ?.build()
      ?: error("Client provider not found for $endpoint")
  }
}
