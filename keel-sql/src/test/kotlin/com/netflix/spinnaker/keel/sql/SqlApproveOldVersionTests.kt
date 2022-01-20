package com.netflix.spinnaker.keel.sql

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.config.PersistenceRetryConfig
import com.netflix.spinnaker.keel.diff.DefaultResourceDiffFactory
import com.netflix.spinnaker.keel.persistence.ApproveOldVersionTests
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.PersistenceRetry
import com.netflix.spinnaker.keel.resources.ResourceFactory
import com.netflix.spinnaker.keel.test.defaultArtifactSuppliers
import com.netflix.spinnaker.keel.test.mockEnvironment
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import io.mockk.mockk
import java.time.Clock

class SqlApproveOldVersionTests : ApproveOldVersionTests<KeelRepository>() {

  private val jooq = testDatabase.context
  private val retryProperties = RetryProperties(1, 0)
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties))
  private val clock = Clock.systemUTC()

  override fun createKeelRepository(resourceFactory: ResourceFactory, mapper: ObjectMapper): KeelRepository {
    val deliveryConfigRepository = SqlDeliveryConfigRepository(jooq, clock, mapper, resourceFactory, sqlRetry, defaultArtifactSuppliers(), publisher = mockk(relaxed = true))
    val resourceRepository = SqlResourceRepository(jooq, clock, mapper, resourceFactory, sqlRetry, publisher = mockk(relaxed = true), spectator = NoopRegistry(), springEnv = mockEnvironment())
    val artifactRepository = SqlArtifactRepository(jooq, clock, mapper, sqlRetry, defaultArtifactSuppliers(), publisher = mockk(relaxed = true))
    val verificationRepository = SqlActionRepository(jooq, clock, mapper, resourceFactory, sqlRetry, environment = mockk())
    return KeelRepository(
      deliveryConfigRepository,
      artifactRepository,
      resourceRepository,
      verificationRepository,
      clock,
      mockk(relaxed = true),
      DefaultResourceDiffFactory(),
      PersistenceRetry(PersistenceRetryConfig())
    )
  }

  override fun flush() {
    SqlTestUtil.cleanupDb(jooq)
  }
}
