package com.netflix.spinnaker.config

import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.ConfigurableEnvironment

@Configuration
@EnableConfigurationProperties(RegistryCacheProperties::class)
class RegistryCacheConfig {
  @Bean
  fun elasticSearchClient(config: RegistryCacheProperties): RestHighLevelClient {
    return RestHighLevelClient(
      RestClient.builder(
        HttpHost(config.host, config.port, "http")
      ).build()
    )
  }
}

@ConfigurationProperties(prefix = "keel.plugins.titus.registry.cache")
class RegistryCacheProperties() {
  var host: String = "configure-me"
  var index: String = "configure-me"
  var port: Int = 9200

  constructor(host: String, index: String, port: Int = 9200) : this() {
    this.host = host
    this.index = index
    this.port = port
  }
}
