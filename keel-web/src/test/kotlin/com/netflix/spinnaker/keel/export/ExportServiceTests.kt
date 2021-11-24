package com.netflix.spinnaker.keel.export

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.spinnaker.config.BaseUrlConfig
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.model.Application
import com.netflix.spinnaker.keel.igor.JobService
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.test.mockEnvironment
import com.netflix.spinnaker.keel.validators.DeliveryConfigValidator
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import io.mockk.coEvery as every
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.assertions.isSuccess

internal class ExportServiceTests {
  private val front50Cache: Front50Cache = mockk()
  private val orcaService: OrcaService = mockk()
  private val yamlMapper: YAMLMapper = mockk()
  private val deliveryConfigRepository: DeliveryConfigRepository = mockk()
  private val jobService: JobService = mockk()

  private val subject = ExportService(
    handlers = listOf(),
    front50Cache = front50Cache,
    orcaService = orcaService,
    baseUrlConfig = BaseUrlConfig(),
    yamlMapper = yamlMapper,
    validator = DeliveryConfigValidator(),
    deliveryConfigRepository = deliveryConfigRepository,
    jobService = jobService,
    springEnv = mockEnvironment(),
    scmConfig = ScmConfig()
  )

  @BeforeEach
  fun setup() {
    every {
      front50Cache.applicationByName("fnord")
    } returns Application(name="fnord", repoType = "stash", repoProjectKey = "spkr", repoSlug = "fnord")

    every {
      deliveryConfigRepository.updateMigratingAppScmStatus("fnord", any())
    } just runs
  }

  @Test
  fun `store application as scm-powered`() {
    every {
      jobService.hasJobs(any(), any(), any(), any())
    } returns true

    expectCatching {
      subject.updateApplicationScmStatus("fnord")
    }.isSuccess()

    verify(exactly = 1) {
      deliveryConfigRepository.updateMigratingAppScmStatus("fnord", true)
    }
  }

  @Test
  fun `store application as non scm-powered`() {
    every {
      jobService.hasJobs(any(), any(), any(), any())
    } returns false

    expectCatching {
      subject.updateApplicationScmStatus("fnord")
    }.isSuccess()

    verify(exactly = 1) {
      deliveryConfigRepository.updateMigratingAppScmStatus("fnord", false)
    }
  }
}
