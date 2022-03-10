package com.netflix.spinnaker.config

import brave.http.HttpTracing
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.config.okhttp3.OkHttpClientProvider
import com.netflix.spinnaker.keel.echo.EchoService
import com.netflix.spinnaker.keel.retrofit.buildRetrofitService
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import org.springframework.beans.factory.BeanCreationException
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class EchoConfiguration {
  @Bean
  fun echoEndpoint(@Value("\${echo.base-url}") echoBaseUrl: String): HttpUrl =
    echoBaseUrl.toHttpUrlOrNull()
      ?: throw BeanCreationException("Invalid URL: $echoBaseUrl")

  @Bean
  fun echoService(
    echoEndpoint: HttpUrl,
    objectMapper: ObjectMapper,
    clientProvider: OkHttpClientProvider,
    httpTracing: HttpTracing
  ): EchoService =
    buildRetrofitService(echoEndpoint, clientProvider, objectMapper, httpTracing)
}
