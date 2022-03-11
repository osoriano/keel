package com.netflix.spinnaker.keel.retrofit

import brave.http.HttpTracing
import brave.okhttp3.TracingCallFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.config.DefaultServiceEndpoint
import com.netflix.spinnaker.config.okhttp3.OkHttpClientProvider
import com.netflix.spinnaker.keel.retrofit.InstrumentedJacksonConverter.Factory
import okhttp3.HttpUrl
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import retrofit2.Retrofit
import retrofit2.Retrofit.Builder

val log: Logger by lazy { LoggerFactory.getLogger("com.netflix.spinnaker.keel.retrofit.Builder") }

/**
 * Creates a Retrofit service of type [T] with our preferred configuration for converters and tracing.
 */
inline fun <reified T : Any> buildRetrofitService(
  endpoint: HttpUrl,
  clientProvider: OkHttpClientProvider,
  objectMapper: ObjectMapper,
  httpTracing: HttpTracing,
  serviceName: String = endpoint.host.substringBefore('.').substringBefore('-'),
): T {
  val type = T::class.java
  return retrofitServiceBuilder(serviceName, endpoint, objectMapper, clientProvider, httpTracing)
    .create(type)
}

inline fun retrofitServiceBuilder(
  serviceName: String,
  endpoint: HttpUrl,
  objectMapper: ObjectMapper,
  clientProvider: OkHttpClientProvider,
  httpTracing: HttpTracing
) : Retrofit {
  val client = clientProvider.getClient(DefaultServiceEndpoint(serviceName, endpoint.toString()))
  val callFactory = TracingCallFactory.create(httpTracing, client)
  log.debug("Creating Retrofit service for $serviceName at $endpoint")
  return Builder()
    .addConverterFactory(Factory(serviceName, objectMapper))
    .baseUrl(endpoint)
    .callFactory(callFactory)
    .build()
}
