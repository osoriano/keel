package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.config.FeatureToggles.Companion.USE_READ_REPLICA
import com.netflix.spinnaker.keel.persistence.ArtifactRepositoryPeriodicallyCheckedTests
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.test.defaultArtifactSuppliers
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.nhaarman.mockitokotlin2.any
import io.mockk.every
import io.mockk.mockk
import java.time.Clock

class SqlArtifactRepositoryPeriodicallyCheckedTests :
  ArtifactRepositoryPeriodicallyCheckedTests<SqlArtifactRepository>() {
  private val jooq = testDatabase.context
  private val objectMapper = configuredObjectMapper()
  private val retryProperties = RetryProperties(1, 0)
  private val featureToggles: FeatureToggles = mockk(relaxed = true) {
    every { isEnabled(USE_READ_REPLICA, any()) } returns true
  }
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties), featureToggles)

  override val factory: (clock: Clock) -> SqlArtifactRepository = { clock ->
    SqlArtifactRepository(jooq, clock, objectMapper, sqlRetry, defaultArtifactSuppliers(), publisher = mockk(relaxed = true))
  }
}
