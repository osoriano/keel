package com.netflix.spinnaker.keel.sql

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.persistence.ApplicationSummaryGenerationTests
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.test.resourceFactory
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import java.time.Clock

class SqlApplicationSummaryGenerationTests : ApplicationSummaryGenerationTests<SqlArtifactRepository>() {
  private val jooq = testDatabase.context
  private val objectMapper = configuredObjectMapper()
  private val retryProperties = RetryProperties(1, 0)
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties))

  private val deliveryConfigRepository = SqlDeliveryConfigRepository(
    jooq,
    Clock.systemUTC(),
    objectMapper,
    resourceFactory(),
    sqlRetry,
    spectator = NoopRegistry(),
  )

  override fun factory(clock: Clock): SqlArtifactRepository =
    SqlArtifactRepository(jooq, clock, objectMapper, sqlRetry, spectator = NoopRegistry())

  override fun SqlArtifactRepository.flush() {
    SqlTestUtil.cleanupDb(jooq)
  }

  override fun persist(manifest: DeliveryConfig) {
    deliveryConfigRepository.store(manifest)
  }
}
