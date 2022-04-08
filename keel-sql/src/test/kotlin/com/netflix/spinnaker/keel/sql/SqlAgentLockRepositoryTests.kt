package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.persistence.AgentLockRepositoryTests
import com.netflix.spinnaker.keel.scheduled.ScheduledAgent
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import io.mockk.every
import io.mockk.mockk
import java.time.Clock

internal object SqlAgentLockRepositoryTests : AgentLockRepositoryTests<SqlAgentLockRepository>() {

  override fun factory(clock: Clock): SqlAgentLockRepository {
    return SqlAgentLockRepository(jooq, clock, listOf(DummyScheduledAgent(1)), sqlRetry)
  }

  private val jooq = testDatabase.context
  private val retryProperties = RetryProperties(1, 0)
  private val featureToggles: FeatureToggles = mockk(relaxed = true) {
    every { isEnabled(FeatureToggles.USE_READ_REPLICA, any()) } returns true
  }
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties), featureToggles)

  override fun SqlAgentLockRepository.flush() {
    SqlTestUtil.cleanupDb(jooq)
  }
}

internal class DummyScheduledAgent(override val lockTimeoutSeconds: Long) : ScheduledAgent {
  override suspend fun invokeAgent() {
  }
}
