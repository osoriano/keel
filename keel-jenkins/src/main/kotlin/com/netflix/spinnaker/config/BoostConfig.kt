package com.netflix.spinnaker.config

import brave.http.HttpTracing
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.config.okhttp3.OkHttpClientProvider
import com.netflix.spinnaker.keel.retrofit.buildRetrofitService
import com.netflix.spinnaker.keel.rocket.BoostApi
import okhttp3.HttpUrl.Companion.toHttpUrl
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * (Rocket) Boost is a Netflix internal service that acts as a wrapper around Jenkins APIs.
 */
@Configuration
class BoostConfig(
  @Value("\${boost.baseUrl}")
  private val baseUrl: String
) {
  @Bean
  fun boostApi(
    objectMapper: ObjectMapper,
    clientProvider: OkHttpClientProvider,
    httpTracing: HttpTracing
  ): BoostApi {
    return buildRetrofitService(baseUrl.toHttpUrl(), clientProvider, objectMapper, httpTracing)
  }
}
