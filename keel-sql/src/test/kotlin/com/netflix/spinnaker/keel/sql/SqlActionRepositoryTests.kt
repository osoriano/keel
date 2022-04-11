package com.netflix.spinnaker.keel.sql

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.artifacts.DockerArtifactSupplier
import com.netflix.spinnaker.keel.jackson.registerKeelApiModule
import com.netflix.spinnaker.keel.persistence.ActionRepositoryTests
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import com.netflix.spinnaker.keel.test.mockEnvironment
import com.netflix.spinnaker.keel.test.resourceFactory
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.AfterEach
import java.time.Instant

internal class SqlActionRepositoryTests :
  ActionRepositoryTests<SqlActionRepository>() {

  private val jooq = testDatabase.context
  private val retryProperties = RetryProperties(1, 0)
  private val featureToggles: FeatureToggles = mockk(relaxed = true) {
    every { isEnabled(FeatureToggles.USE_READ_REPLICA, any()) } returns true
  }
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties), featureToggles)
  private val artifactSuppliers = listOf(DockerArtifactSupplier(mockk(), mockk(), mockk()))

  private val mapper = configuredTestObjectMapper()
    .apply {
      registerSubtypes(NamedType(DockerArtifact::class.java, DOCKER))
    }
    .registerKeelApiModule()
    .apply {
      registerSubtypes(NamedType(DummyVerification::class.java, "dummyVerification"))
      registerSubtypes(NamedType(DummyPostDeployAction::class.java, "dummyPostDeploy"))
    }

  private val deliveryConfigRepository = SqlDeliveryConfigRepository(
    jooq = jooq,
    clock = clock,
    objectMapper = mapper,
    resourceFactory = resourceFactory(),
    sqlRetry = sqlRetry,
    artifactSuppliers = artifactSuppliers,
    publisher = mockk(relaxed = true),
    featureToggles = mockk()
  )
  private val artifactRepository = SqlArtifactRepository(
    jooq = jooq,
    clock = clock,
    objectMapper = mapper,
    sqlRetry = sqlRetry,
    artifactSuppliers = artifactSuppliers,
    publisher = mockk(relaxed = true)
  )

  private val pausedRepository = SqlPausedRepository(
    jooq = jooq,
    sqlRetry = sqlRetry,
    clock = clock
  )

  override fun createSubject() =
    SqlActionRepository(
      jooq = jooq,
      clock = clock,
      resourceFactory = mockk(),
      objectMapper = mapper,
      sqlRetry = sqlRetry,
      artifactSuppliers = artifactSuppliers,
      environment = mockEnvironment()
    )

  override fun ArtifactInEnvironmentContext.setup() {
    deliveryConfig.artifacts.forEach(artifactRepository::register)
    deliveryConfigRepository.store(deliveryConfig)
    artifactRepository.storeArtifactVersion(
      PublishedArtifact(
        artifact.name,
        artifact.type,
        version,
        createdAt = Instant.now()
      )
    )
  }

  override fun ArtifactInEnvironmentContext.setupCurrentArtifactVersion() {
    artifactRepository.markAsSuccessfullyDeployedTo(
      deliveryConfig,
      artifact,
      version,
      environmentName
    )
  }

  override fun ArtifactInEnvironmentContext.pauseApplication() {
    pausedRepository.pauseApplication(
      deliveryConfig.application,
      "fzlem@netflix.com"
    )
  }

  @AfterEach
  fun flush() {
    SqlTestUtil.cleanupDb(jooq)
  }
}
