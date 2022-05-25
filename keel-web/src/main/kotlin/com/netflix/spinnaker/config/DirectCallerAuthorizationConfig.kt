package com.netflix.spinnaker.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spinnaker.security.DirectCallerAuthorizationFilter
import com.netflix.spinnaker.security.config.DirectCallerAuthorizationProperties
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.web.servlet.FilterRegistrationBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import

@EnableConfigurationProperties(KeelDirectCallerAuthorizationProperties::class)
@ConditionalOnProperty("netflix.sso.direct-caller-authorization-filter.enabled", havingValue = "true", matchIfMissing = false)
@Import(DirectCallerAuthorizationFilter::class)
@Configuration
class DirectCallerAuthorizationConfig {
  @Bean
  fun directCallerFilterProps(
    keelDirectCallerAuthorizationProperties: KeelDirectCallerAuthorizationProperties,
    objectMapper: ObjectMapper
  ): DirectCallerAuthorizationProperties {
    // copy the direct caller authz props into the right type for the filter
    // (we do this because some Spring init ordering problem prevents the right type from
    // loading the config from keel.yml)
    return objectMapper.convertValue(keelDirectCallerAuthorizationProperties)
  }

  @Bean
  fun directCallerFilter(
    filter: DirectCallerAuthorizationFilter
  ): FilterRegistrationBean<DirectCallerAuthorizationFilter> {
    val frb = FilterRegistrationBean(filter)
    frb.order = -98 // after auth which is 100
    return frb
  }
}
