package com.netflix.spinnaker.keel.okhttp

import com.netflix.spinnaker.config.okhttp3.OkHttpClientBuilderProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class KeelOkhttpConfiguration {
  @Bean
  fun mdcAwareOkhttpClientProvider(
    clientProviders: List<OkHttpClientBuilderProvider>
  ) = MdcAwareOkhttpClientProvider(clientProviders)
}
