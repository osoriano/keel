package com.netflix.spinnaker.keel.sql

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.persistence.ArtifactRepositoryPromotionFlowTests
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import com.netflix.spinnaker.keel.test.defaultArtifactSuppliers
import com.netflix.spinnaker.keel.test.resourceFactory
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil.cleanupDb
import org.junit.jupiter.api.AfterEach
import org.springframework.context.ApplicationEventPublisher
import java.time.Clock

class SqlArtifactRepositoryPromotionFlowTests : ArtifactRepositoryPromotionFlowTests<SqlArtifactRepository>() {
  private val jooq = testDatabase.context
  private val objectMapper = configuredTestObjectMapper()
  private val retryProperties = RetryProperties(1, 0)
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties))

  private val deliveryConfigRepository = SqlDeliveryConfigRepository(
    jooq,
    Clock.systemUTC(),
    objectMapper,
    resourceFactory(),
    sqlRetry,
    defaultArtifactSuppliers(),
    spectator = NoopRegistry()
  )

  override fun factory(clock: Clock): SqlArtifactRepository =
    SqlArtifactRepository(jooq, clock, objectMapper, sqlRetry, defaultArtifactSuppliers(), NoopRegistry())

  override fun SqlArtifactRepository.flush() {
    cleanupDb(jooq)
  }

  override fun persist(manifest: DeliveryConfig) {
    deliveryConfigRepository.store(manifest)
  }


  @AfterEach
  fun flush() {
    cleanupDb(jooq)
  }
}
