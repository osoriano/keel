package com.netflix.spinnaker.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.netflix.spinnaker.config.okhttp3.OkHttpClientProvider
import com.netflix.spinnaker.keel.retrofit.InstrumentedJacksonConverter
import com.netflix.spinnaker.keel.rocket.BoostApi
import okhttp3.HttpUrl.Companion.toHttpUrl
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory

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
    clientProvider: OkHttpClientProvider
  ): BoostApi {
    return Retrofit.Builder()
      .addConverterFactory(InstrumentedJacksonConverter.Factory("boost", objectMapper))
      .baseUrl(baseUrl.toHttpUrl())
      .client(clientProvider.getClient(DefaultServiceEndpoint("boost", baseUrl)))
      .build()
      .create(BoostApi::class.java)
  }
}
