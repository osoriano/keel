package com.netflix.spinnaker.keel.sql

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.scm.CommitCreatedEvent
import com.netflix.spinnaker.keel.jackson.registerKeelApiModule
import com.netflix.spinnaker.keel.persistence.WorkQueueRepositoryTests
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import io.mockk.every
import io.mockk.mockk

internal class SqlWorkQueueRepositoryTests: WorkQueueRepositoryTests<SqlWorkQueueRepository>() {
  private val jooq = testDatabase.context
  private val retryProperties = RetryProperties(1, 0)
  private val featureToggles: FeatureToggles = mockk(relaxed = true) {
    every { isEnabled(FeatureToggles.USE_READ_REPLICA, any()) } returns true
  }
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties), featureToggles)

  private val mapper = configuredObjectMapper()
    .registerKeelApiModule()
    .apply {
      registerSubtypes(NamedType(CommitCreatedEvent::class.java, "commit.created"))
    }



  override fun createSubject(): SqlWorkQueueRepository =
    SqlWorkQueueRepository(
      jooq = jooq,
      clock = clock,
      mapper = mapper,
      sqlRetry = sqlRetry,
    )
}
