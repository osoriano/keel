package com.netflix.spinnaker.keel.dgs

import com.netflix.spinnaker.keel.core.api.ActuationPlan
import com.netflix.spinnaker.keel.graphql.types.MD_InitiateApplicationMigrationPayload
import com.netflix.spinnaker.keel.migrations.ApplicationPrData
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.services.ApplicationService
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import com.netflix.spinnaker.keel.test.submittedDeliveryConfig
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isNotNull
import java.time.Instant
import io.mockk.coEvery as every
import io.mockk.coVerify as verify

internal class MigrationTests {
  private val deliveryConfigRepository: DeliveryConfigRepository = mockk()
  private val applicationService: ApplicationService = mockk()
  private val mapper = configuredTestObjectMapper()
  private val migration = Migration(deliveryConfigRepository, applicationService, mapper)

  private val app = "test"
  private val user = "user"
  private val migrationData = ApplicationPrData(
    deliveryConfig = submittedDeliveryConfig(),
    repoSlug = "repo",
    projectKey = "project"
  )

  @BeforeEach
  fun setup() {
    every {
      applicationService.openMigrationPr(any(), any())
    } returns (migrationData to "http://link-to-pr")

    every {
      applicationService.storePausedMigrationConfig(any(), any())
    } returns migrationData.deliveryConfig.toDeliveryConfig()

    every {
      applicationService.getActuationPlan(any())
    } returns ActuationPlan(
      application = app,
      timestamp = Instant.now(),
      environmentPlans = emptyList()
    )
  }

  @Test
  fun `initiating a migration generates a PR, stores the paused config, and returns the actuation plan`() {
    val migration = migration.md_initiateApplicationMigration(MD_InitiateApplicationMigrationPayload(app), user)

    verify { applicationService.openMigrationPr(app, user) }
    verify { applicationService.storePausedMigrationConfig(app, user) }
    expectThat(migration?.actuationPlan).isNotNull()
  }
}
