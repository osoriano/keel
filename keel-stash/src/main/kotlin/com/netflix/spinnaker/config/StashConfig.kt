package com.netflix.spinnaker.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.config.okhttp3.OkHttpClientProvider
import com.netflix.spinnaker.keel.retrofit.InstrumentedJacksonConverter
import com.netflix.spinnaker.keel.stash.StashService
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import org.springframework.beans.factory.BeanCreationException
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import retrofit2.Retrofit
import retrofit2.converter.scalars.ScalarsConverterFactory

@Configuration
class StashConfig {
  @Bean
  fun stashEndpoint(@Value("\${stash.base-url}") stashBaseUrl: String): HttpUrl =
    stashBaseUrl.toHttpUrlOrNull()
      ?: throw BeanCreationException("Invalid URL: $stashBaseUrl")

  @Bean
  fun stashService(
    stashEndpoint: HttpUrl,
    objectMapper: ObjectMapper,
    clientProvider: OkHttpClientProvider
  ): StashService =
    Retrofit.Builder()
      .addConverterFactory(ScalarsConverterFactory.create())
      .addConverterFactory(InstrumentedJacksonConverter.Factory("Stash", objectMapper))
      .baseUrl(stashEndpoint)
      .client(clientProvider.getClient(DefaultServiceEndpoint("stash", stashEndpoint.toString())))
      .build()
      .create(StashService::class.java)
}
