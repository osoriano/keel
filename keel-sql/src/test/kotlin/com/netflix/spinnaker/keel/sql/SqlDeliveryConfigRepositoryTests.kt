package com.netflix.spinnaker.keel.sql

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.config.ResourceEventPruneConfig
import com.netflix.spinnaker.keel.core.api.DependsOnConstraint
import com.netflix.spinnaker.keel.core.api.ManualJudgementConstraint
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepositoryTests
import com.netflix.spinnaker.keel.resources.ResourceSpecIdentifier
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import com.netflix.spinnaker.keel.test.defaultArtifactSuppliers
import com.netflix.spinnaker.keel.test.mockEnvironment
import com.netflix.spinnaker.keel.test.resourceFactory
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil.cleanupDb
import com.netflix.spinnaker.time.MutableClock
import io.mockk.mockk
import org.junit.jupiter.api.BeforeAll
import org.springframework.context.ApplicationEventPublisher
import java.time.Clock

internal object SqlDeliveryConfigRepositoryTests : DeliveryConfigRepositoryTests<SqlDeliveryConfigRepository, SqlResourceRepository, SqlArtifactRepository, SqlPausedRepository>() {
  private val jooq = testDatabase.context
  private val objectMapper = configuredTestObjectMapper()
  private val retryProperties = RetryProperties(1, 0)
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties))
  private var heart: SqlHeart? = null

  override fun createDeliveryConfigRepository(resourceSpecIdentifier: ResourceSpecIdentifier, publisher: ApplicationEventPublisher, clock: MutableClock, featureToggles: FeatureToggles): SqlDeliveryConfigRepository {
    heart = SqlHeart(jooq, sqlRetry, clock)
    return SqlDeliveryConfigRepository(
        jooq,
        clock,
        objectMapper,
        resourceFactory(resourceSpecIdentifier),
        sqlRetry,
        defaultArtifactSuppliers(),
        publisher = publisher,
        featureToggles = featureToggles
    )
  }

  override fun createResourceRepository(resourceSpecIdentifier: ResourceSpecIdentifier, publisher: ApplicationEventPublisher, clock: MutableClock): SqlResourceRepository =
    SqlResourceRepository(jooq, clock, objectMapper, resourceFactory(resourceSpecIdentifier), sqlRetry, publisher, NoopRegistry(), springEnv = mockEnvironment(), resourceEventPruneConfig = ResourceEventPruneConfig())

  override fun createArtifactRepository(publisher: ApplicationEventPublisher, clock: MutableClock): SqlArtifactRepository =
    SqlArtifactRepository(
      jooq,
      clock,
      objectMapper,
      sqlRetry,
      defaultArtifactSuppliers(),
      publisher = publisher
    )

  override fun createPausedRepository(): SqlPausedRepository =
    SqlPausedRepository(jooq, sqlRetry, Clock.systemUTC())

  override fun beat() {
    heart?.beat()
  }

  override fun clearBeats() =
    heart?.cleanOldRecords() ?: -1

  override fun flush() {
    cleanupDb(jooq)
  }

  @JvmStatic
  @BeforeAll
  fun registerConstraintSubtypes() {
    with(objectMapper) {
      registerSubtypes(NamedType(DependsOnConstraint::class.java, "depends-on"))
      registerSubtypes(NamedType(ManualJudgementConstraint::class.java, "manual-judgement"))
      registerSubtypes(NamedType(DummyVerification::class.java, "verification"))
    }
  }
}
