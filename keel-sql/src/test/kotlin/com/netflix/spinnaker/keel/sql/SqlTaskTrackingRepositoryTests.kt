package com.netflix.spinnaker.keel.sql

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.keel.persistence.TaskTrackingRepositoryTests
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import io.mockk.mockk
import java.time.Clock
import org.springframework.context.ApplicationEventPublisher

internal object SqlTaskTrackingRepositoryTests : TaskTrackingRepositoryTests<SqlTaskTrackingRepository>() {

  private val jooq = testDatabase.context
  private val retryProperties = RetryProperties(1, 0)
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties))

  override fun factory(clock: Clock): SqlTaskTrackingRepository {
    return SqlTaskTrackingRepository(
      jooq,
      Clock.systemUTC(),
      sqlRetry,
      publisher = mockk(),
      spectator = NoopRegistry()
    )
  }

  override fun SqlTaskTrackingRepository.flush() {
    SqlTestUtil.cleanupDb(jooq)
  }
}
