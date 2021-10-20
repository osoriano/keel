package com.netflix.spinnaker.config

import com.netflix.spinnaker.keel.api.ResourceDiffFactory
import com.netflix.spinnaker.keel.diff.DefaultResourceDiffFactory
import com.netflix.spinnaker.keel.diff.IdentityServiceCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class ObjectDifferConfig(private val identityServiceCustomizers: List<IdentityServiceCustomizer>) {
  @Bean
  fun resourceDiffFactory(): ResourceDiffFactory =
    DefaultResourceDiffFactory(identityServiceCustomizers)
}
