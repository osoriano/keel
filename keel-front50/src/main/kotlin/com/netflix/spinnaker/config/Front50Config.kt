package com.netflix.spinnaker.config

import brave.http.HttpTracing
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.config.okhttp3.OkHttpClientProvider
import com.netflix.spinnaker.keel.front50.Front50Service
import com.netflix.spinnaker.keel.retrofit.buildRetrofitService
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import org.springframework.beans.factory.BeanCreationException
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConditionalOnProperty("front50.enabled")
class Front50Config {
  @Bean
  fun front50Endpoint(@Value("\${front50.base-url}") front50BaseUrl: String): HttpUrl =
    front50BaseUrl.toHttpUrlOrNull()
      ?: throw BeanCreationException("Invalid URL: $front50BaseUrl")

  @Bean
  fun front50Service(
    front50Endpoint: HttpUrl,
    objectMapper: ObjectMapper,
    clientProvider: OkHttpClientProvider,
    httpTracing: HttpTracing
  ): Front50Service =
    buildRetrofitService(front50Endpoint, clientProvider, objectMapper, httpTracing)
}
