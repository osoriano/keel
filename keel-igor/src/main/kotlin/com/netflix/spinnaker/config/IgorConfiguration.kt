package com.netflix.spinnaker.config

import brave.http.HttpTracing
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.spinnaker.config.okhttp3.OkHttpClientProvider
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.igor.BuildService
import com.netflix.spinnaker.keel.igor.DeliveryConfigImporter
import com.netflix.spinnaker.keel.igor.ScmService
import com.netflix.spinnaker.keel.igor.artifact.ArtifactService
import com.netflix.spinnaker.keel.retrofit.buildRetrofitService
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import org.springframework.beans.factory.BeanCreationException
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class IgorConfiguration {
  @Bean
  fun igorEndpoint(@Value("\${igor.base-url}") igorBaseUrl: String): HttpUrl =
    igorBaseUrl.toHttpUrlOrNull()
      ?: throw BeanCreationException("Invalid URL: $igorBaseUrl")

  @Bean
  fun artifactService(
    igorEndpoint: HttpUrl,
    objectMapper: ObjectMapper,
    clientProvider: OkHttpClientProvider,
    httpTracing: HttpTracing
  ): ArtifactService = buildRetrofitService(igorEndpoint, clientProvider, objectMapper, httpTracing)

  @Bean
  fun scmService(
    igorEndpoint: HttpUrl,
    objectMapper: ObjectMapper,
    clientProvider: OkHttpClientProvider,
    httpTracing: HttpTracing
  ): ScmService = buildRetrofitService(igorEndpoint, clientProvider, objectMapper, httpTracing)

  @Bean
  fun buildService(
    igorEndpoint: HttpUrl,
    objectMapper: ObjectMapper,
    clientProvider: OkHttpClientProvider,
    httpTracing: HttpTracing
  ): BuildService = buildRetrofitService(igorEndpoint, clientProvider, objectMapper, httpTracing)

  @Bean
  fun deliveryConfigImporter(
    scmService: ScmService,
    front50Cache: Front50Cache,
    yamlMapper: YAMLMapper,
  ) = DeliveryConfigImporter(scmService, front50Cache, yamlMapper)
}
