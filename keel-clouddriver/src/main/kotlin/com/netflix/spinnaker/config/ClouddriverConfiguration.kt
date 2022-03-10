package com.netflix.spinnaker.config

import brave.http.HttpTracing
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.config.okhttp3.OkHttpClientProvider
import com.netflix.spinnaker.keel.caffeine.CacheFactory
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.clouddriver.ImageService
import com.netflix.spinnaker.keel.clouddriver.MemoryCloudDriverCache
import com.netflix.spinnaker.keel.retrofit.buildRetrofitService
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import org.springframework.beans.factory.BeanCreationException
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment

@Configuration
@ConditionalOnProperty("clouddriver.enabled")
class ClouddriverConfiguration {

  @Bean
  fun clouddriverEndpoint(@Value("\${clouddriver.base-url}") clouddriverBaseUrl: String): HttpUrl =
    clouddriverBaseUrl.toHttpUrlOrNull()
      ?: throw BeanCreationException("Invalid URL: $clouddriverBaseUrl")

  @Bean
  @ConditionalOnMissingBean(CloudDriverService::class)
  fun clouddriverService(
    clouddriverEndpoint: HttpUrl,
    objectMapper: ObjectMapper,
    clientProvider: OkHttpClientProvider,
    httpTracing: HttpTracing
  ) : CloudDriverService =
      buildRetrofitService(clouddriverEndpoint, clientProvider,  objectMapper, httpTracing)

  @Bean
  @ConditionalOnMissingBean(CloudDriverCache::class)
  fun cloudDriverCache(
    cloudDriverService: CloudDriverService,
    cacheFactory: CacheFactory
  ) =
    MemoryCloudDriverCache(cloudDriverService, cacheFactory)

  @Bean
  fun imageService(
    cloudDriverService: CloudDriverService,
    cacheFactory: CacheFactory,
    springEnv: Environment
  ) = ImageService(cloudDriverService, cacheFactory, springEnv)
}
