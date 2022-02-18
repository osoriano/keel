package com.netflix.spinnaker.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.config.okhttp3.OkHttpClientProvider
import com.netflix.spinnaker.keel.retrofit.InstrumentedJacksonConverter
import com.netflix.spinnaker.keel.stash.StashService
import okhttp3.Credentials
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.Response
import org.springframework.beans.factory.BeanCreationException
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import retrofit2.Retrofit
import retrofit2.converter.scalars.ScalarsConverterFactory

@Configuration
@EnableConfigurationProperties(StashProperties::class)
class StashConfig {

  @Bean
  fun stashEndpoint(@Value("\${stash.base-url}") stashBaseUrl: String): HttpUrl =
    stashBaseUrl.toHttpUrlOrNull()
      ?: throw BeanCreationException("Invalid URL: $stashBaseUrl")

  @Bean
  fun stashService(
    stashEndpoint: HttpUrl,
    objectMapper: ObjectMapper,
    clientProvider: OkHttpClientProvider,
    config: StashProperties
  ): StashService {
    val username = config.username
    val password = config.password
    return Retrofit.Builder()
      .addConverterFactory(ScalarsConverterFactory.create())
      .addConverterFactory(InstrumentedJacksonConverter.Factory("Stash", objectMapper))
      .baseUrl(stashEndpoint)
      .client(OkHttpClient.Builder()
        .addInterceptor(BasicAuthRequestInterceptor(username, password))
        .build()
      )
      .build()
      .create(StashService::class.java)
  }

  class BasicAuthRequestInterceptor(
    private val username: String,
    private val password: String
  ): Interceptor {

    override fun intercept(chain: Interceptor.Chain): Response {
      val request = chain.request().newBuilder()
        .header("Authorization",  Credentials.basic(username, password))
        .build()
      return chain.proceed(request)
    }
  }
}

  @ConfigurationProperties("stash")
  class StashProperties {
    var username = "<fill-me-in>"
    var password = "<fill-me-in>"
  }
