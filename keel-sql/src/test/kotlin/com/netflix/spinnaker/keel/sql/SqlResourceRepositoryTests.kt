package com.netflix.spinnaker.keel.sql

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.config.ResourceEventPruneConfig
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.persistence.ResourceRepositoryTests
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.telemetry.ResourceCheckCompleted
import com.netflix.spinnaker.keel.test.mockEnvironment
import com.netflix.spinnaker.keel.test.resourceFactory
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil.cleanupDb
import io.mockk.every
import io.mockk.mockk
import org.springframework.context.ApplicationEventPublisher
import java.time.Clock
import java.time.Duration

internal class SqlResourceRepositoryTests : ResourceRepositoryTests<SqlResourceRepository>() {
  private val jooq = testDatabase.context
  private val retryProperties = RetryProperties(5, 100)
  private val featureToggles: FeatureToggles = mockk(relaxed = true) {
    every { isEnabled(FeatureToggles.USE_READ_REPLICA, any()) } returns true
  }
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties), featureToggles)
  private val resourceFactory = resourceFactory()
  private val resourceEventPruneConfig = ResourceEventPruneConfig().apply {
    minEventsKept = 10
    deleteChunkSize = 5
    daysKept = 1
  }
  private val deliveryConfigRepository = SqlDeliveryConfigRepository(
      jooq,
      clock,
      configuredObjectMapper(),
      resourceFactory,
      sqlRetry,
      publisher = mockk(relaxed = true),
      featureToggles = mockk()
  )

  override fun factory(clock: Clock, publisher: ApplicationEventPublisher): SqlResourceRepository {
    return SqlResourceRepository(
      jooq,
      clock,
      configuredObjectMapper(),
      resourceFactory,
      sqlRetry,
      publisher,
      NoopRegistry(),
      springEnv = mockEnvironment(),
      resourceEventPruneConfig
    )
  }

  override val storeDeliveryConfig: (DeliveryConfig) -> Unit = deliveryConfigRepository::store

  override fun flush() {
    cleanupDb(jooq)
  }

  /**
   * Allows us to call this on the typed repository w/o exposing this method on the interface.
   */
  override fun pruneHistory(resourceId: String, repository: SqlResourceRepository) {
    repository.pruneHistory(ResourceCheckCompleted(Duration.ZERO, resourceId))
  }
}
