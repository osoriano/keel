package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.persistence.NotificationRepositoryTests
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import io.mockk.every
import io.mockk.mockk
import java.time.Clock

internal class SqlNotificationRepositoryTests : NotificationRepositoryTests<SqlNotificationRepository>() {

  private val jooq = testDatabase.context
  private val retryProperties = RetryProperties(1, 0)
  private val featureToggles: FeatureToggles = mockk(relaxed = true) {
    every { isEnabled(FeatureToggles.USE_READ_REPLICA, any()) } returns true
  }
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties), featureToggles)

  override fun factory(clock: Clock): SqlNotificationRepository {
    return SqlNotificationRepository(
      jooq,
      clock,
      sqlRetry
    )
  }

  override fun SqlNotificationRepository.flush() {
    SqlTestUtil.cleanupDb(jooq)
  }
}
