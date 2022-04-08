package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.persistence.BakedImageRepositoryTests
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import io.mockk.every
import io.mockk.mockk
import java.time.Clock

class SqlBakedImageRepositoryTests : BakedImageRepositoryTests<SqlBakedImageRepository>() {

  private val jooq = testDatabase.context
  private val objectMapper = configuredTestObjectMapper()
  private val retryProperties = RetryProperties(1, 0)
  private val featureToggles: FeatureToggles = mockk(relaxed = true) {
    every { isEnabled(FeatureToggles.USE_READ_REPLICA, any()) } returns true
  }
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties), featureToggles)

  override fun factory(clock: Clock): SqlBakedImageRepository =
    SqlBakedImageRepository(jooq, clock, objectMapper, sqlRetry)


  override fun SqlBakedImageRepository.flush() {
    SqlTestUtil.cleanupDb(jooq)
  }
}
