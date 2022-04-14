package com.netflix.spinnaker.keel.sql

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.persistence.LifecycleMonitorRepositoryTests
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import io.mockk.every
import io.mockk.mockk
import org.springframework.context.ApplicationEventPublisher
import java.time.Clock

internal object SqlLifecycleMonitorRepositoryTests
  : LifecycleMonitorRepositoryTests<SqlLifecycleMonitorRepository, SqlLifecycleEventRepository>() {
  private val jooq = testDatabase.context
  private val retryProperties = RetryProperties(1, 0)
  private val featureToggles: FeatureToggles = mockk(relaxed = true) {
    every { isEnabled(FeatureToggles.USE_READ_REPLICA, any()) } returns true
  }
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties), featureToggles)

  override fun monitorFactory(clock: Clock): SqlLifecycleMonitorRepository {
    return SqlLifecycleMonitorRepository(
      jooq,
      clock,
      sqlRetry
    )
  }

  override fun eventFactory(clock: Clock, publisher: ApplicationEventPublisher): SqlLifecycleEventRepository {
    return SqlLifecycleEventRepository(
      clock,
      jooq,
      sqlRetry,
      NoopRegistry(),
      publisher
    )
  }

  override fun SqlLifecycleMonitorRepository.flush() {
    SqlTestUtil.cleanupDb(jooq)
  }
  override fun SqlLifecycleEventRepository.flush() {
    SqlTestUtil.cleanupDb(jooq)
  }
}
