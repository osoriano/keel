package com.netflix.spinnaker.config

import brave.http.HttpTracing
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.config.okhttp3.OkHttpClientProvider
import com.netflix.spinnaker.keel.jira.JiraService
import com.netflix.spinnaker.keel.retrofit.buildRetrofitService
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import org.springframework.beans.factory.BeanCreationException
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

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
    clientProvider: OkHttpClientProvider,
    httpTracing: HttpTracing
  ): JiraService =
    buildRetrofitService(jiraEndpoint, clientProvider, objectMapper, httpTracing)
}
