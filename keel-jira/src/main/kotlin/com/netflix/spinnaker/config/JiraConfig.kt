package com.netflix.spinnaker.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.config.okhttp3.OkHttpClientProvider
import com.netflix.spinnaker.keel.jira.JiraService
import com.netflix.spinnaker.keel.retrofit.InstrumentedJacksonConverter
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import org.springframework.beans.factory.BeanCreationException
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import retrofit2.Retrofit

@Configuration
class JiraConfig {
  @Bean
  fun jiraEndpoint(@Value("\${jira.base-url}") jiraBaseUrl: String): HttpUrl =
    jiraBaseUrl.toHttpUrlOrNull()
      ?: throw BeanCreationException("Invalid URL: $jiraBaseUrl")

  @Bean
  fun jiraService(
    jiraEndpoint: HttpUrl,
    objectMapper: ObjectMapper,
    clientProvider: OkHttpClientProvider
  ): JiraService =
    Retrofit.Builder()
      .addConverterFactory(InstrumentedJacksonConverter.Factory("Jira", objectMapper))
      .baseUrl(jiraEndpoint)
      .client(clientProvider.getClient(DefaultServiceEndpoint("jira", jiraEndpoint.toString())))
      .build()
      .create(JiraService::class.java)
}
