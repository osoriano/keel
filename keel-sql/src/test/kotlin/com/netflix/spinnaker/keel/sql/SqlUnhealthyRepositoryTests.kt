package com.netflix.spinnaker.keel.sql

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.persistence.UnhealthyRepositoryTests
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.test.mockEnvironment
import com.netflix.spinnaker.keel.test.resourceFactory
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import java.time.Clock

internal object SqlUnhealthyRepositoryTests : UnhealthyRepositoryTests<SqlUnhealthyRepository>() {
  private val jooq = testDatabase.context
  private val retryProperties = RetryProperties(1, 0)
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties))
  private val resourceFactory = resourceFactory()
  private val resourceRepository = SqlResourceRepository(
    jooq,
    clock,
    configuredObjectMapper(),
    resourceFactory,
    sqlRetry,
    spectator = NoopRegistry(),
    springEnv = mockEnvironment()
  )

  override fun factory(clock: Clock): SqlUnhealthyRepository =
    SqlUnhealthyRepository(
      clock,
      jooq,
      sqlRetry
    )

  override fun store(resource: Resource<*>) {
    resourceRepository.store(resource)
  }

  override fun flush() {
    SqlTestUtil.cleanupDb(jooq)
  }
}
