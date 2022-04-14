package com.netflix.spinnaker.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier
import com.netflix.spinnaker.keel.events.PersistentEvent.Companion.clock
import com.netflix.spinnaker.keel.resources.ResourceFactory
import com.netflix.spinnaker.keel.scheduled.ScheduledAgent
import com.netflix.spinnaker.keel.sql.SqlActionRepository
import com.netflix.spinnaker.keel.sql.SqlAgentLockRepository
import com.netflix.spinnaker.keel.sql.SqlApplicationRepository
import com.netflix.spinnaker.keel.sql.SqlArtifactRepository
import com.netflix.spinnaker.keel.sql.SqlBakedImageRepository
import com.netflix.spinnaker.keel.sql.SqlDeliveryConfigRepository
import com.netflix.spinnaker.keel.sql.SqlDiffFingerprintRepository
import com.netflix.spinnaker.keel.sql.SqlDismissibleNotificationRepository
import com.netflix.spinnaker.keel.sql.SqlEnvironmentDeletionRepository
import com.netflix.spinnaker.keel.sql.SqlEnvironmentLeaseRepository
import com.netflix.spinnaker.keel.sql.SqlFeatureRolloutRepository
import com.netflix.spinnaker.keel.sql.SqlHeart
import com.netflix.spinnaker.keel.sql.SqlLifecycleEventRepository
import com.netflix.spinnaker.keel.sql.SqlLifecycleMonitorRepository
import com.netflix.spinnaker.keel.sql.SqlNotificationRepository
import com.netflix.spinnaker.keel.sql.SqlPausedRepository
import com.netflix.spinnaker.keel.sql.SqlResourceRepository
import com.netflix.spinnaker.keel.sql.SqlRetry
import com.netflix.spinnaker.keel.sql.SqlTaskTrackingRepository
import com.netflix.spinnaker.keel.sql.SqlUnhappyVetoRepository
import com.netflix.spinnaker.keel.sql.SqlUnhealthyRepository
import com.netflix.spinnaker.keel.sql.SqlWorkQueueRepository
import com.netflix.spinnaker.kork.sql.config.DefaultSqlConfiguration
import com.netflix.spinnaker.kork.sql.config.SqlProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import org.jooq.DSLContext
import org.jooq.impl.DefaultConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.core.env.Environment
import java.time.Clock
import javax.annotation.PostConstruct

@Configuration
@ConditionalOnProperty("sql.enabled")
@EnableConfigurationProperties(RetentionProperties::class, ResourceEventPruneConfig::class)
@Import(DefaultSqlConfiguration::class, SqlRetryProperties::class, EnvironmentExclusionConfig::class)
class SqlConfiguration
{

  @Autowired
  lateinit var jooqConfiguration: DefaultConfiguration

  @Autowired
  lateinit var sqlRetryProperties: SqlRetryProperties

  @Autowired
  lateinit var environmentExclusionConfig: EnvironmentExclusionConfig

  // This allows us to run tests with a testcontainers database that has a different schema name to
  // the real one used by the JOOQ code generator. It _is_ possible to change the schema used by
  // testcontainers but not when initializing the database with just the JDBC connection string
  // which is super convenient, especially for Spring integration tests.
  @PostConstruct
  fun tweakJooqConfiguration() {
    jooqConfiguration.settings().isRenderSchema = false
  }

  @Bean
  fun applicationRepository(
    jooq: DSLContext,
    clock: Clock,
    objectMapper: ObjectMapper,
    featureToggles: FeatureToggles
  ) = SqlApplicationRepository(jooq,  clock, SqlRetry(sqlRetryProperties, featureToggles))

  @Bean
  fun resourceRepository(
    jooq: DSLContext,
    clock: Clock,
    resourceFactory: ResourceFactory,
    objectMapper: ObjectMapper,
    publisher: ApplicationEventPublisher,
    registry: Registry,
    springEnv: Environment,
    resourceEventPruneConfig: ResourceEventPruneConfig,
    featureToggles: FeatureToggles
  ) =
    SqlResourceRepository(
      jooq,
      clock,
      objectMapper,
      resourceFactory,
      SqlRetry(sqlRetryProperties, featureToggles),
      publisher,
      registry,
      springEnv,
      resourceEventPruneConfig
    )

  @Bean
  fun artifactRepository(
    jooq: DSLContext,
    clock: Clock,
    objectMapper: ObjectMapper,
    artifactSuppliers: List<ArtifactSupplier<*, *>>,
    publisher: ApplicationEventPublisher,
    featureToggles: FeatureToggles
  ) =
    SqlArtifactRepository(
      jooq,
      clock,
      objectMapper,
      SqlRetry(sqlRetryProperties, featureToggles),
      artifactSuppliers,
      publisher
    )

  @Bean
  fun deliveryConfigRepository(
    jooq: DSLContext,
    clock: Clock,
    resourceFactory: ResourceFactory,
    objectMapper: ObjectMapper,
    artifactSuppliers: List<ArtifactSupplier<*, *>>,
    publisher: ApplicationEventPublisher,
    featureToggles: FeatureToggles
  ) =
    SqlDeliveryConfigRepository(
        jooq = jooq,
        clock = clock,
        objectMapper = objectMapper,
        resourceFactory = resourceFactory,
        sqlRetry = SqlRetry(sqlRetryProperties, featureToggles),
        artifactSuppliers = artifactSuppliers,
        publisher = publisher,
        featureToggles = featureToggles
    )

  @Bean
  fun diffFingerprintRepository(
    jooq: DSLContext,
    clock: Clock,
    featureToggles: FeatureToggles
  ) = SqlDiffFingerprintRepository(jooq, clock, SqlRetry(sqlRetryProperties, featureToggles))

  @Bean
  fun unhappyVetoRepository(
    jooq: DSLContext,
    featureToggles: FeatureToggles
  ) =
    SqlUnhappyVetoRepository(clock, jooq, SqlRetry(sqlRetryProperties, featureToggles))

  @Bean
  fun pausedRepository(
    jooq: DSLContext,
    featureToggles: FeatureToggles
  ) = SqlPausedRepository(jooq, SqlRetry(sqlRetryProperties, featureToggles), clock)

  @Bean
  fun taskTrackingRepository(
    jooq: DSLContext,
    clock: Clock,
    retentionProperties: RetentionProperties,
    featureToggles: FeatureToggles
  ) = SqlTaskTrackingRepository(jooq, clock, SqlRetry(sqlRetryProperties, featureToggles), retentionProperties)

  @Bean
  fun agentLockRepository(
    jooq: DSLContext,
    clock: Clock,
    properties: SqlProperties,
    agents: List<ScheduledAgent>,
    featureToggles: FeatureToggles
  ) = SqlAgentLockRepository(jooq, clock, agents, SqlRetry(sqlRetryProperties, featureToggles))

  @Bean
  fun notificationRepository(
    jooq: DSLContext,
    clock: Clock,
    properties: SqlProperties,
    featureToggles: FeatureToggles
  ) = SqlNotificationRepository(jooq, clock, SqlRetry(sqlRetryProperties, featureToggles))

  @Bean
  fun unhealthyRepository(
    jooq: DSLContext,
    clock: Clock,
    properties: SqlProperties,
    featureToggles: FeatureToggles
  ) = SqlUnhealthyRepository(clock, jooq, SqlRetry(sqlRetryProperties, featureToggles))

  @Bean
  fun actionRepository(
    jooq: DSLContext,
    clock: Clock,
    resourceFactory: ResourceFactory,
    objectMapper: ObjectMapper,
    artifactSuppliers: List<ArtifactSupplier<*, *>>,
    environment: Environment,
    featureToggles: FeatureToggles
  ) = SqlActionRepository(
    jooq,
    clock,
    objectMapper,
    resourceFactory,
    SqlRetry(sqlRetryProperties, featureToggles),
    artifactSuppliers,
    environment
  )

  @Bean
  fun lifecycleEventRepository(
    jooq: DSLContext,
    clock: Clock,
    properties: SqlProperties,
    objectMapper: ObjectMapper,
    spectator: Registry,
    publisher: ApplicationEventPublisher,
    featureToggles: FeatureToggles
  ) =
    SqlLifecycleEventRepository(clock, jooq, SqlRetry(sqlRetryProperties, featureToggles), spectator, publisher)

  @Bean
  fun lifecycleMonitorRepository(
    jooq: DSLContext,
    clock: Clock,
    properties: SqlProperties,
    objectMapper: ObjectMapper,
    featureToggles: FeatureToggles
  ) = SqlLifecycleMonitorRepository(jooq, clock, SqlRetry(sqlRetryProperties, featureToggles))

  @Bean
  fun bakedImageRepository(
    jooq: DSLContext,
    clock: Clock,
    properties: SqlProperties,
    objectMapper: ObjectMapper,
    featureToggles: FeatureToggles
  ) = SqlBakedImageRepository(jooq, clock, objectMapper, SqlRetry(sqlRetryProperties, featureToggles))

  @Bean
  fun environmentLeaseRepository(
    jooq: DSLContext,
    clock: Clock,
    registry: Registry
  ) = SqlEnvironmentLeaseRepository(jooq, clock, registry, environmentExclusionConfig.leaseDuration)

  @Bean
  fun dismissibleNotificationRepository(
    jooq: DSLContext,
    clock: Clock,
    objectMapper: ObjectMapper,
    featureToggles: FeatureToggles
  ) = SqlDismissibleNotificationRepository(jooq, SqlRetry(sqlRetryProperties, featureToggles), objectMapper, clock)

  @Bean
  fun environmentDeletionRepository(
    jooq: DSLContext,
    clock: Clock,
    resourceFactory: ResourceFactory,
    objectMapper: ObjectMapper,
    artifactSuppliers: List<ArtifactSupplier<*, *>>,
    featureToggles: FeatureToggles
  ) = SqlEnvironmentDeletionRepository(
    jooq,
    clock,
    objectMapper,
    SqlRetry(sqlRetryProperties, featureToggles),
    resourceFactory,
    artifactSuppliers
  )

  @Bean
  fun artifactProcessingRepository(
    jooq: DSLContext,
    clock: Clock,
    objectMapper: ObjectMapper,
    featureToggles: FeatureToggles
  ) = SqlWorkQueueRepository(jooq, clock, objectMapper, SqlRetry(sqlRetryProperties, featureToggles))

  @Bean
  fun featureRolloutRepository(
    jooq: DSLContext,
    clock: Clock,
    featureToggles: FeatureToggles
  ) = SqlFeatureRolloutRepository(jooq, SqlRetry(sqlRetryProperties, featureToggles), clock)

  @Bean
  fun heart(
    jooq: DSLContext,
    clock: Clock,
    featureToggles: FeatureToggles
  ) = SqlHeart(jooq, SqlRetry(sqlRetryProperties, featureToggles), clock)
}
