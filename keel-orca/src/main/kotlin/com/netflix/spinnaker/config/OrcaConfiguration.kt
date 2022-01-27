package com.netflix.spinnaker.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.config.okhttp3.OkHttpClientProvider
import com.netflix.spinnaker.keel.orca.DryRunCapableOrcaService
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.retrofit.InstrumentedJacksonConverter
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import retrofit2.Retrofit

@Configuration
@ConditionalOnProperty("orca.enabled")
@ComponentScan("com.netflix.spinnaker.keel.orca")
class OrcaConfiguration {

  @Bean
  fun orcaEndpoint(@Value("\${orca.base-url}") orcaBaseUrl: String) =
    orcaBaseUrl.toHttpUrlOrNull()

  @Bean
  fun orcaService(
    orcaEndpoint: HttpUrl,
    objectMapper: ObjectMapper,
    clientProvider: OkHttpClientProvider,
    springEnv: Environment
  ): OrcaService {
    val endpoint = DefaultServiceEndpoint("orca", orcaEndpoint.toString())
    val client = clientProvider.getClient(endpoint)
    return DryRunCapableOrcaService(
      springEnv = springEnv,
      delegate = Retrofit.Builder()
        .baseUrl(orcaEndpoint)
        .client(client)
        .addConverterFactory(InstrumentedJacksonConverter.Factory("Orca", objectMapper))
        .build()
        .create(OrcaService::class.java)
    )
  }
}
