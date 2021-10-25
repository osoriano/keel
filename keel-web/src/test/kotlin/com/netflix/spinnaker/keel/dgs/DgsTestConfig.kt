package com.netflix.spinnaker.keel.dgs

import com.netflix.spinnaker.keel.bakery.BakeryMetadataService
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import io.mockk.mockk
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

@Configuration
@ComponentScan(basePackages = ["com.netflix.spinnaker.keel.dgs"])
class DgsTestConfig {

  val cloudDriverService: CloudDriverService = mockk(relaxUnitFun = true)
  val bakeryMetadataService: BakeryMetadataService = mockk(relaxUnitFun = true)

  @Bean
  fun applicationFetcherSupport() = ApplicationFetcherSupport(cloudDriverService, bakeryMetadataService)

}

