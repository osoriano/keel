package com.netflix.spinnaker.keel.dgs

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.netflix.graphql.dgs.DgsDataFetchingEnvironment
import com.netflix.spinnaker.keel.core.api.ActuationPlan
import com.netflix.spinnaker.keel.services.ApplicationService
import com.netflix.spinnaker.keel.test.DummyResourceSpec
import com.netflix.spinnaker.keel.test.TEST_API_V1
import com.netflix.spinnaker.keel.test.configuredTestYamlMapper
import com.netflix.spinnaker.keel.test.submittedDeliveryConfig
import io.mockk.coEvery as every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.assertions.isEqualTo
import strikt.assertions.isSuccess
import java.time.Instant

internal class DryRunTests {
  private val yamlMapper = configuredTestYamlMapper().apply {
    registerSubtypes(NamedType(DummyResourceSpec::class.java, TEST_API_V1.qualify("whatever").toString()))
  }
  private val applicationService: ApplicationService = mockk()
  private val subject = DryRun(applicationService, yamlMapper)
  private val submittedDeliveryConfig = submittedDeliveryConfig()
  private val dataFetchingEnvironment: DgsDataFetchingEnvironment = mockk()

  @Test
  fun `delegates the dry-run to the application service`() {
    val actuationPlan = ActuationPlan(submittedDeliveryConfig.application, Instant.now(), emptyList())

    every {
      applicationService.dryRun(any())
    } returns actuationPlan

    expectCatching {
      subject.dryRun(dataFetchingEnvironment, submittedDeliveryConfig.let { yamlMapper.writeValueAsString(it) })
    }.isSuccess()
      .isEqualTo(actuationPlan.toDgs(completed = true))
  }
}
