package com.netflix.spinnaker.keel.sql

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.config.FeatureToggles.Companion.USE_READ_REPLICA
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.NPM
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.artifacts.NpmArtifact
import com.netflix.spinnaker.keel.persistence.ArtifactRepositoryPeriodicallyCheckedTests
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.test.defaultArtifactSuppliers
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import io.mockk.every
import io.mockk.mockk
import java.time.Clock

class SqlArtifactRepositoryPeriodicallyCheckedTests :
  ArtifactRepositoryPeriodicallyCheckedTests<SqlArtifactRepository>() {
  private val jooq = testDatabase.context
  private val objectMapper = configuredObjectMapper().apply {
    registerSubtypes(
      NamedType(DebianArtifact::class.java, DEBIAN),
      NamedType(DockerArtifact::class.java, DOCKER),
      NamedType(NpmArtifact::class.java, NPM)
    )
  }
  private val retryProperties = RetryProperties(1, 0)
  private val featureToggles: FeatureToggles = mockk(relaxed = true) {
    every { isEnabled(USE_READ_REPLICA, any()) } returns true
  }
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties), featureToggles)

  override val factory: (clock: Clock) -> SqlArtifactRepository = { clock ->
    SqlArtifactRepository(
      jooq,
      clock,
      objectMapper,
      sqlRetry,
      defaultArtifactSuppliers(),
      publisher = mockk(relaxed = true)
    )
  }
}
