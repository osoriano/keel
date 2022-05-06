package com.netflix.spinnaker.keel.sql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.kotlin.OpenClass
import com.netflix.spinnaker.keel.activation.ApplicationUp
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.GitRepository
import com.netflix.spinnaker.keel.api.artifacts.ArtifactMetadata
import com.netflix.spinnaker.keel.api.artifacts.ArtifactStatus
import com.netflix.spinnaker.keel.api.artifacts.ArtifactType
import com.netflix.spinnaker.keel.api.artifacts.CurrentlyDeployedVersion
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier
import com.netflix.spinnaker.keel.api.plugins.supporting
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.artifacts.StatusInfoForArtifactInEnvironment
import com.netflix.spinnaker.keel.core.api.ActionMetadata
import com.netflix.spinnaker.keel.core.api.ArtifactSummaryInEnvironment
import com.netflix.spinnaker.keel.core.api.ArtifactVersionStatus
import com.netflix.spinnaker.keel.core.api.ArtifactVersionVetoData
import com.netflix.spinnaker.keel.core.api.ArtifactVersions
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactPin
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVeto
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVetoes
import com.netflix.spinnaker.keel.core.api.EnvironmentSummary
import com.netflix.spinnaker.keel.core.api.PinnedEnvironment
import com.netflix.spinnaker.keel.core.api.PromotionStatus
import com.netflix.spinnaker.keel.core.api.PromotionStatus.APPROVED
import com.netflix.spinnaker.keel.core.api.PromotionStatus.CURRENT
import com.netflix.spinnaker.keel.core.api.PromotionStatus.DEPLOYING
import com.netflix.spinnaker.keel.core.api.PromotionStatus.PENDING
import com.netflix.spinnaker.keel.core.api.PromotionStatus.PREVIOUS
import com.netflix.spinnaker.keel.core.api.PromotionStatus.SKIPPED
import com.netflix.spinnaker.keel.core.api.PromotionStatus.VETOED
import com.netflix.spinnaker.keel.core.api.PublishedArtifactInEnvironment
import com.netflix.spinnaker.keel.core.api.randomUID
import com.netflix.spinnaker.keel.persistence.ArtifactNotFoundException
import com.netflix.spinnaker.keel.persistence.ArtifactRepository
import com.netflix.spinnaker.keel.persistence.NoSuchArtifactException
import com.netflix.spinnaker.keel.persistence.NoSuchArtifactVersionException
import com.netflix.spinnaker.keel.persistence.NoSuchDeploymentException
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ACTIVE_ENVIRONMENT
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ARTIFACT_LAST_CHECKED
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ARTIFACT_PROMOTION_EVENT
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ARTIFACT_VERSIONS
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.DELIVERY_ARTIFACT
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.DELIVERY_CONFIG
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ENVIRONMENT
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ENVIRONMENT_ARTIFACT_PIN
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ENVIRONMENT_ARTIFACT_VERSIONS
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ENVIRONMENT_ARTIFACT_VETO
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ENVIRONMENT_LAST_POST_DEPLOY
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ENVIRONMENT_LAST_VERIFIED
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ENVIRONMENT_VERSION_ARTIFACT_VERSION
import com.netflix.spinnaker.keel.sql.RetryCategory.READ
import com.netflix.spinnaker.keel.sql.RetryCategory.WRITE
import com.netflix.spinnaker.keel.telemetry.AboutToBeChecked
import de.huxhorn.sulky.ulid.ULID
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Record1
import org.jooq.Select
import org.jooq.SelectConditionStep
import org.jooq.SelectJoinStep
import org.jooq.impl.DSL
import org.jooq.impl.DSL.select
import org.jooq.impl.DSL.selectOne
import org.jooq.util.mysql.MySQLDSL
import org.slf4j.LoggerFactory
import org.springframework.cloud.sleuth.annotation.NewSpan
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import java.security.MessageDigest
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.Instant.EPOCH
import javax.xml.bind.DatatypeConverter

@JvmDefaultWithoutCompatibility
@OpenClass
class SqlArtifactRepository(
  private val jooq: DSLContext,
  private val clock: Clock,
  private val objectMapper: ObjectMapper,
  private val sqlRetry: SqlRetry,
  private val artifactSuppliers: List<ArtifactSupplier<*, *>> = emptyList(),
  private val publisher: ApplicationEventPublisher
) : ArtifactRepository {

  override fun register(artifact: DeliveryArtifact): Boolean {
    val (isNew, id) = sqlRetry.withRetry(READ) {
      jooq
        .select(DELIVERY_ARTIFACT.UID)
        .from(DELIVERY_ARTIFACT)
        .where(
          DELIVERY_ARTIFACT.TYPE.eq(artifact.type)
            .and(DELIVERY_ARTIFACT.DELIVERY_CONFIG_NAME.eq(artifact.deliveryConfigName))
            .and(DELIVERY_ARTIFACT.REFERENCE.eq(artifact.reference))
        )
        .fetchOne(DELIVERY_ARTIFACT.UID)
    }
      ?.let { false to it }
      ?: true to randomUID().toString()

    sqlRetry.withRetry(WRITE) {
      jooq.insertInto(DELIVERY_ARTIFACT)
        .set(DELIVERY_ARTIFACT.UID, id)
        .set(DELIVERY_ARTIFACT.FINGERPRINT, artifact.fingerprint())
        .set(DELIVERY_ARTIFACT.NAME, artifact.name)
        .set(DELIVERY_ARTIFACT.TYPE, artifact.type)
        .set(DELIVERY_ARTIFACT.REFERENCE, artifact.reference)
        .set(DELIVERY_ARTIFACT.DELIVERY_CONFIG_NAME, artifact.deliveryConfigName)
        .set(DELIVERY_ARTIFACT.IS_PREVIEW, artifact.isPreview)
        .set(DELIVERY_ARTIFACT.DETAILS, artifact.detailsAsJson())
        .onDuplicateKeyUpdate()
        .set(DELIVERY_ARTIFACT.NAME, artifact.name)
        .set(DELIVERY_ARTIFACT.DETAILS, artifact.detailsAsJson())
        .execute()
      jooq.insertInto(ARTIFACT_LAST_CHECKED)
        .set(ARTIFACT_LAST_CHECKED.ARTIFACT_UID, id)
        .set(ARTIFACT_LAST_CHECKED.AT, EPOCH.plusSeconds(1))
        .onDuplicateKeyUpdate()
        .set(ARTIFACT_LAST_CHECKED.AT, EPOCH.plusSeconds(1))
        .execute()
    }
    return isNew
  }

  private fun DeliveryArtifact.detailsAsJson(): String {
    val details = objectMapper.convertValue<Map<String, Any?>>(this)
      .toMutableMap()
      // remove all the basic fields that have their own columns; everything else is serialized
      // as one json blob in the `details` column
      .also {
        it.remove("name")
        it.remove("deliveryConfigName")
        it.remove("type")
        it.remove("reference")
      }

    return objectMapper.writeValueAsString(details)
  }

  override fun get(name: String, type: ArtifactType, deliveryConfigName: String): List<DeliveryArtifact> =
    sqlRetry.withRetry(READ) {
      jooq
        .select(DELIVERY_ARTIFACT.DETAILS, DELIVERY_ARTIFACT.REFERENCE, DELIVERY_ARTIFACT.IS_PREVIEW)
        .from(DELIVERY_ARTIFACT)
        .where(DELIVERY_ARTIFACT.NAME.eq(name))
        .and(DELIVERY_ARTIFACT.TYPE.eq(type))
        .and(DELIVERY_ARTIFACT.DELIVERY_CONFIG_NAME.eq(deliveryConfigName))
        .fetch { (details, reference, isPreview) ->
          objectMapper.mapToArtifact(
            artifactSuppliers.supporting(type),
            name,
            type,
            details,
            reference,
            deliveryConfigName,
            isPreview
          )
        }
    }

  override fun get(name: String, type: ArtifactType, reference: String, deliveryConfigName: String): DeliveryArtifact =
    sqlRetry.withRetry(READ) {
      jooq
        .select(DELIVERY_ARTIFACT.DETAILS, DELIVERY_ARTIFACT.REFERENCE, DELIVERY_ARTIFACT.IS_PREVIEW)
        .from(DELIVERY_ARTIFACT)
        .where(DELIVERY_ARTIFACT.NAME.eq(name))
        .and(DELIVERY_ARTIFACT.TYPE.eq(type))
        .and(DELIVERY_ARTIFACT.DELIVERY_CONFIG_NAME.eq(deliveryConfigName))
        .and(DELIVERY_ARTIFACT.REFERENCE.eq(reference))
        .fetchOne()
    }
      ?.let { (details, reference, isPreview) ->
        objectMapper.mapToArtifact(artifactSuppliers.supporting(type), name, type, details, reference, deliveryConfigName, isPreview)
      } ?: throw ArtifactNotFoundException(reference, deliveryConfigName)

  override fun get(deliveryConfigName: String, reference: String): DeliveryArtifact {
    return sqlRetry.withRetry(READ) {
      jooq
        .select(
          DELIVERY_ARTIFACT.NAME,
          DELIVERY_ARTIFACT.DETAILS,
          DELIVERY_ARTIFACT.REFERENCE,
          DELIVERY_ARTIFACT.TYPE,
          DELIVERY_ARTIFACT.IS_PREVIEW
        )
        .from(DELIVERY_ARTIFACT)
        .where(
          DELIVERY_ARTIFACT.DELIVERY_CONFIG_NAME.eq(deliveryConfigName),
          DELIVERY_ARTIFACT.REFERENCE.eq(reference)
        )
        .fetchOne()
    }
      ?.let { (name, details, reference, type, isPreview) ->
        objectMapper.mapToArtifact(artifactSuppliers.supporting(type), name, type, details, reference, deliveryConfigName, isPreview)
      } ?: throw ArtifactNotFoundException(reference, deliveryConfigName)
  }

  override fun delete(artifact: DeliveryArtifact) {
    requireNotNull(artifact.deliveryConfigName) { "Error removing artifact - it has no delivery config!" }
    jooq.transaction { config ->
      val txn = DSL.using(config)
      txn.deleteFrom(DELIVERY_ARTIFACT)
        .where(DELIVERY_ARTIFACT.UID.eq(artifact.uid))
        .execute()
    }
  }

  override fun isRegistered(name: String, type: ArtifactType): Boolean =
    sqlRetry.withRetry(READ) {
      jooq
        .selectCount()
        .from(DELIVERY_ARTIFACT)
        .where(DELIVERY_ARTIFACT.NAME.eq(name))
        .and(DELIVERY_ARTIFACT.TYPE.eq(type))
        .fetchSingle()
        .value1()
    } > 0

  override fun getAll(type: ArtifactType?, name: String?): List<DeliveryArtifact> =
    sqlRetry.withRetry(READ) {
      jooq
        .select(
          DELIVERY_ARTIFACT.NAME,
          DELIVERY_ARTIFACT.TYPE,
          DELIVERY_ARTIFACT.DETAILS,
          DELIVERY_ARTIFACT.REFERENCE,
          DELIVERY_ARTIFACT.DELIVERY_CONFIG_NAME,
          DELIVERY_ARTIFACT.IS_PREVIEW
        )
        .from(DELIVERY_ARTIFACT)
        .apply { if (type != null) where(DELIVERY_ARTIFACT.TYPE.eq(type.toString())) }
        .apply { if (name != null) where(DELIVERY_ARTIFACT.NAME.eq(name)) }
        .fetch { (name, storedType, details, reference, configName, isPreview) ->
          objectMapper.mapToArtifact(
            artifactSuppliers.supporting(storedType),
            name,
            storedType.lowercase(),
            details,
            reference,
            configName,
            isPreview
          )
        }
    }

  override fun versions(artifact: DeliveryArtifact, limit: Int): List<PublishedArtifact> {
    val retry = Retry.of(
      "artifact registered",
      RetryConfig.custom<Boolean>()
        // retry a couple times since this is invoked from ArtifactListener and there's a race
        // condition with persisting the delivery config on upsert.
        .maxAttempts(5)
        .waitDuration(Duration.ofMillis(100))
        .retryOnResult { registered ->
          if (!registered) {
            log.debug("Retrying registered check for $artifact")
            true
          } else {
            false
          }
        }
        .build()
    )

    val registered = retry.executeSupplier {
      isRegistered(artifact.name, artifact.type)
    }

    if (!registered) {
      throw NoSuchArtifactException(artifact)
    }

    return sqlRetry.withRetry(READ) {
      jooq
        .selectArtifactVersionColumns()
        .from(ARTIFACT_VERSIONS)
        .where(ARTIFACT_VERSIONS.NAME.eq(artifact.name))
        .and(ARTIFACT_VERSIONS.TYPE.eq(artifact.type))
        .fetchSortedArtifactVersions(artifact, limit)
        .map { it.copy(reference = artifact.reference) }
    }
  }

  override fun getVersionsWithIncompleteMetadata(limit: Int, maxAge: Duration): List<PublishedArtifact> {
    val cutoff = clock.instant().minus(maxAge)

    return sqlRetry.withRetry(READ) {
      jooq
        .selectArtifactVersionColumns()
        .from(ARTIFACT_VERSIONS)
        .where(ARTIFACT_VERSIONS.CREATED_AT.greaterOrEqual(cutoff))
        // TODO: is filling in the reference important here? if so, we can join on DELIVERY_ARTIFACT
        .fetchArtifactVersions(null)
        .filter {
          it.buildMetadata?.job == null
        } // we save partial metadata, so we need to filter after we load
        .take(limit)
    }
  }

  override fun storeArtifactVersion(artifactVersion: PublishedArtifact): Boolean {
    with(artifactVersion) {
      if (!isRegistered(name, type)) {
        throw NoSuchArtifactException(name, type)
      }

      return sqlRetry.withRetry(WRITE) {
        jooq.insertInto(ARTIFACT_VERSIONS)
          .set(ARTIFACT_VERSIONS.NAME, name)
          .set(ARTIFACT_VERSIONS.TYPE, type)
          .set(ARTIFACT_VERSIONS.VERSION, version)
          .set(ARTIFACT_VERSIONS.RELEASE_STATUS, status)
          .set(ARTIFACT_VERSIONS.CREATED_AT, createdAt)
          .set(ARTIFACT_VERSIONS.STORED_AT, clock.instant())
          .set(ARTIFACT_VERSIONS.GIT_METADATA, gitMetadata)
          .set(ARTIFACT_VERSIONS.BUILD_METADATA, buildMetadata)
          .set(ARTIFACT_VERSIONS.ORIGINAL_METADATA, objectMapper.writeValueAsString(artifactVersion.metadata))
          .onDuplicateKeyUpdate()
          .set(ARTIFACT_VERSIONS.GIT_METADATA, gitMetadata)
          .set(ARTIFACT_VERSIONS.BUILD_METADATA, buildMetadata)
          .execute()
      } == 1
    }
  }

  override fun getArtifactVersion(
    artifact: DeliveryArtifact,
    version: String,
    status: ArtifactStatus?
  ): PublishedArtifact? {
    return sqlRetry.withRetry(READ) {
      jooq
        .selectArtifactVersionColumns()
        .from(ARTIFACT_VERSIONS)
        .where(ARTIFACT_VERSIONS.NAME.eq(artifact.name))
        .and(ARTIFACT_VERSIONS.TYPE.eq(artifact.type))
        .and(ARTIFACT_VERSIONS.VERSION.eq(version))
        .apply { if (status != null) and(ARTIFACT_VERSIONS.RELEASE_STATUS.eq(status)) }
        .fetchArtifactVersions(artifact.reference)
        .firstOrNull()
    }
  }

  override fun updateArtifactMetadata(artifact: PublishedArtifact, artifactMetadata: ArtifactMetadata) {
    with(artifact) {
      if (!isRegistered(name, type)) {
        throw NoSuchArtifactException(name, type)
      }

      sqlRetry.withRetry(WRITE) {
        jooq.update(ARTIFACT_VERSIONS)
          .set(ARTIFACT_VERSIONS.BUILD_METADATA, artifactMetadata.buildMetadata)
          .set(ARTIFACT_VERSIONS.GIT_METADATA, artifactMetadata.gitMetadata)
          .where(
            ARTIFACT_VERSIONS.NAME.eq(name),
            ARTIFACT_VERSIONS.TYPE.eq(type),
            ARTIFACT_VERSIONS.VERSION.eq(version).or(ARTIFACT_VERSIONS.VERSION.eq("$name-$version"))
          )
          .apply { if (status != null) and(ARTIFACT_VERSIONS.RELEASE_STATUS.eq(status)) }
          .execute()
      }
    }
  }

  override fun getReleaseStatus(artifact: DeliveryArtifact, version: String): ArtifactStatus? =
    if (isRegistered(artifact.name, artifact.type)) {
      sqlRetry.withRetry(READ) {
        jooq
          .select(ARTIFACT_VERSIONS.RELEASE_STATUS)
          .from(ARTIFACT_VERSIONS)
          .where(ARTIFACT_VERSIONS.NAME.eq(artifact.name))
          .and(ARTIFACT_VERSIONS.TYPE.eq(artifact.type))
          .and(ARTIFACT_VERSIONS.VERSION.eq(version))
          .fetchOne(ARTIFACT_VERSIONS.RELEASE_STATUS)
      }
    } else {
      throw NoSuchArtifactException(artifact)
    }

  override fun latestDeployableVersionIn(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    targetEnvironment: String
  ): String? {
    val environment = deliveryConfig.environmentNamed(targetEnvironment)
    val envUid = deliveryConfig.getUidFor(environment)
    val artifactId = artifact.uid

    return sqlRetry.withRetry(READ) {
      jooq
        .selectArtifactVersionColumns()
        .from(ARTIFACT_VERSIONS, ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(ARTIFACT_VERSIONS.VERSION))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(envUid))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifactId))
        .and(ARTIFACT_VERSIONS.NAME.eq(artifact.name)) // match name also, so if the artifact has been updated we match only the correct versions
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.APPROVED_AT.isNotNull)
        .and(
          ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.`in`(
            DEPLOYING,
            CURRENT,
            PREVIOUS
          )
        ) // these statuses indicate an artifact was already selected as a desired version
        .fetchSortedArtifactVersions(artifact, 20)
        .firstOrNull()
        ?.version
    }
  }

  override fun latestVersionApprovedIn(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    targetEnvironment: String,
  ): String? {
    val environment = deliveryConfig.environmentNamed(targetEnvironment)
    val envUid = deliveryConfig.getUidFor(environment)
    val artifactId = artifact.uid

    /**
     * If [targetEnvironment] has been pinned to an artifact version, return
     * the pinned version. Otherwise return the most recently approved version.
     *
     */
    sqlRetry.withRetry(READ) {
      jooq.select(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_VERSION)
        .from(ENVIRONMENT_ARTIFACT_PIN)
        .where(
          ENVIRONMENT_ARTIFACT_PIN.ENVIRONMENT_UID.eq(envUid),
          ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_UID.eq(artifactId)
        )
        .fetchOne(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_VERSION)
    }
      ?.also { return it }

    return latestApprovedArtifact(envUid, artifactId, listOf(VETOED), artifact)?.version
  }

  private fun latestApprovedArtifact(envUid: Select<Record1<String>>, artifactId: Select<Record1<String>>, excludeStatuses: List<PromotionStatus>, artifact: DeliveryArtifact): PublishedArtifact? {
    return sqlRetry.withRetry(READ) {
      jooq
        .selectArtifactVersionColumns()
        .from(ARTIFACT_VERSIONS, ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(ARTIFACT_VERSIONS.VERSION))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(envUid))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifactId))
        .and(ARTIFACT_VERSIONS.NAME.eq(artifact.name)) // match name also, so if the artifact has been updated we match only the correct versions
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.APPROVED_AT.isNotNull)
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.notIn(excludeStatuses))
        .orderBy(ENVIRONMENT_ARTIFACT_VERSIONS.APPROVED_AT.desc())
        .limit(20)
        .fetchArtifactVersions(artifact.reference)
        .sortedWith(artifact.sortingStrategy.comparator)
        .firstOrNull()
    }
  }

  /**
   * @return all versions that are approved and newer than current. If there is no current in the
   * ENVIRONMENT_ARTIFACT_VERSIONS table (because it was vetoed) then take all the approved versions.
   * This function is used when evaluating versions for deployment.
   *
   * deployment candidates are:
   * - if there has been a version previously selected for deployment (current, deploying) then anything newer than that version.
   *
   */
  override fun deploymentCandidateVersions(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    targetEnvironment: String
  ): List<String> {
    return sqlRetry.withRetry(READ) {
      // we need to get approved versions newer than the latest version that has already started deploying
      val candidateVersions = getArtifactVersionsByStatus(
        deliveryConfig, artifact.reference, targetEnvironment, listOf(APPROVED, DEPLOYING, CURRENT)
      )

      candidateVersions
        .sortedByDescending {
          // we assume that sorting all artifacts by created at time will give us accurate ordering information
          it.createdAt
        }
        .takeWhile {
          // this key is added to the metadata in `getArtifactVersionsByStatus`
          // all approved versions newer than the latest deploying version are candidates
          it.metadata["statusInEnvironment"] == APPROVED.name
        }
        .map { it.version }
    }
  }

  /**
   * Approves a version in an environment.
   * return true if the version was not already approved.
   */
  override fun approveVersionFor(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String
  ): Boolean {
    val environment = deliveryConfig.environmentNamed(targetEnvironment)
    return sqlRetry.withRetry(WRITE) {
      val alreadyApproved = isApprovedFor(deliveryConfig, artifact, version, targetEnvironment)

      when {
        alreadyApproved -> false // version did not change, so we return false
        else -> {
          val result = jooq
            .insertInto(ENVIRONMENT_ARTIFACT_VERSIONS)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID, deliveryConfig.getUidFor(environment))
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID, artifact.uid)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION, version)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.APPROVED_AT, clock.instant())
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, APPROVED)
            .onDuplicateKeyUpdate()
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.APPROVED_AT, clock.instant())
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, APPROVED)
            .setNull(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY)
            .setNull(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_AT)
            .execute() > 0
          recordEnvArtifactVersionEvent(deliveryConfig, environment, artifact, version, APPROVED, jooq)
          result
        }
      }
    }
  }

  override fun isApprovedFor(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String
  ): Boolean {
    val environment = deliveryConfig.environmentNamed(targetEnvironment)
    return sqlRetry.withRetry(READ) {
      jooq
        .fetchExists(
          ENVIRONMENT_ARTIFACT_VERSIONS,
          ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(environment.uid)
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(version))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.notIn(listOf(VETOED, SKIPPED)))
        )
    }
  }

  override fun getApprovedAt(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String
  ): Instant? {
    val environment = deliveryConfig.environmentNamed(targetEnvironment)
    return sqlRetry.withRetry(READ) {
      jooq
        .select(ENVIRONMENT_ARTIFACT_VERSIONS.APPROVED_AT)
        .from(ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(environment.uid))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(version))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.APPROVED_AT.isNotNull)
        .fetchOne(ENVIRONMENT_ARTIFACT_VERSIONS.APPROVED_AT)
    }
  }

  override fun getPinnedAt(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String
  ): Instant? {
    val environment = deliveryConfig.environmentNamed(targetEnvironment)
    return sqlRetry.withRetry(READ) {
      jooq
        .select(ENVIRONMENT_ARTIFACT_PIN.PINNED_AT)
        .from(ENVIRONMENT_ARTIFACT_PIN)
        .where(ENVIRONMENT_ARTIFACT_PIN.ENVIRONMENT_UID.eq(environment.uid))
        .and(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_UID.eq(artifact.uid))
        .and(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_VERSION.eq(version))
        .fetchOne(ENVIRONMENT_ARTIFACT_PIN.PINNED_AT)
    }
  }

  override fun wasSuccessfullyDeployedTo(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String
  ): Boolean {
    val environment = deliveryConfig.environmentNamed(targetEnvironment)
    val result = sqlRetry.withRetry(READ) {
      jooq
        .fetchExists(
          CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS,
          CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(deliveryConfig.getUidFor(environment))
            .and(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
            .and(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(version))
        ) || jooq
        .fetchExists(
          ENVIRONMENT_ARTIFACT_VERSIONS,
          ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(deliveryConfig.getUidFor(environment))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(version))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT.isNotNull)
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.`in`(listOf(CURRENT, PREVIOUS)))
        )
    }

    return result
  }

  override fun isCurrentlyDeployedTo(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String
  ): Boolean =
    getCurrentlyDeployedArtifactVersionId(deliveryConfig, artifact, targetEnvironment)?.version == version

  /**
   * @return the most recent version to be marked as current, or null if none exist
   *
   * adds deployedAt time to the metadata
   */
  override fun getCurrentlyDeployedArtifactVersion(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    environmentName: String
  ): PublishedArtifact? =
    getCurrentlyDeployedArtifactVersionId(deliveryConfig, artifact, environmentName)
      ?.let { (version, deployedAt) ->
        val publishedArtifact = getArtifactVersion(artifact, version, null)
        val originalMetadata = publishedArtifact?.metadata ?: emptyMap()
        val deployedAtMetadata = mapOf("deployedAt" to deployedAt)
        return publishedArtifact?.copy(metadata = originalMetadata + deployedAtMetadata, reference = artifact.reference)
      }

  /**
   * @return the version string of the currently deployed artifact in the specified environment,
   * and the time it was deployed
   */
  override fun getCurrentlyDeployedArtifactVersionId(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    environmentName: String
  ): CurrentlyDeployedVersion? {
    val environment = deliveryConfig.environmentNamed(environmentName)
    return sqlRetry.withRetry(READ) {
      jooq.select(
        CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION,
        CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT
      )
        .from(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(environment.uid))
        .and(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
        .orderBy(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT.desc())
        .limit(1)
        .fetch { (version, deployedAt) ->
          CurrentlyDeployedVersion(version, deployedAt)
        }
    }.firstOrNull()
  }

  override fun markAsDeployingTo(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String
  ) {
    val environment = deliveryConfig.environmentNamed(targetEnvironment)
    val environmentUid = deliveryConfig.getUidFor(environment)
    val now = clock.instant()
    sqlRetry.withRetry(WRITE) {
      jooq.transaction { config ->
        val txn = DSL.using(config)
        val stuckVersions = txn.select(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION)
          .from(ENVIRONMENT_ARTIFACT_VERSIONS)
          .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(environmentUid))
          .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
          .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.eq(DEPLOYING))
          .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.ne(version))
          .fetch(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION)
        if (stuckVersions.isNotEmpty()) {
          log.error("Stuck deploying versions ${stuckVersions.joinToString()} for artifact '${artifact.reference}' in delivery config ${deliveryConfig.name} found when deploying version $version")
          txn.update(ENVIRONMENT_ARTIFACT_VERSIONS)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, SKIPPED)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY, version)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_AT, now)
            .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(environmentUid))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.eq(DEPLOYING))
            .execute()
          batchRecordEnvArtifactVersionEvent(deliveryConfig, environment, artifact, stuckVersions, SKIPPED, txn)
        }

        txn
          .insertInto(ENVIRONMENT_ARTIFACT_VERSIONS)
          .set(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID, environmentUid)
          .set(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID, artifact.uid)
          .set(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION, version)
          .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, DEPLOYING)
          .onDuplicateKeyUpdate()
          .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, DEPLOYING)
          .execute()
        recordEnvArtifactVersionEvent(deliveryConfig, environment, artifact, version, DEPLOYING, txn)
      }
    }
  }

  override fun isDeployingTo(
    deliveryConfig: DeliveryConfig,
    targetEnvironment: String
  ): Boolean =
    jooq.selectCount()
      .from(ENVIRONMENT_ARTIFACT_VERSIONS)
      .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(deliveryConfig.getUidFor(targetEnvironment)))
      .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.eq(DEPLOYING))
      .fetchSingleInto<Int>() > 0

  /**
   * Mark current version as successfully deployed if the version is not already marked as [CURRENT]
   * @return true if the version is newly marked as deployed
   */
  override fun markAsSuccessfullyDeployedTo(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String
  ): Boolean {
    val environment = deliveryConfig.environmentNamed(targetEnvironment)
    val environmentUid = deliveryConfig.getUidFor(environment)
    val environmentUidString = deliveryConfig.getUidStringFor(environment)
    val artifactUid = artifact.uidString
    val artifactVersion = getArtifactVersion(artifact, version)
      ?: throw NoSuchArtifactVersionException(artifact, version)

    return sqlRetry.withRetry(WRITE) {
      if (getArtifactPromotionStatus(deliveryConfig, artifact, version, targetEnvironment) == CURRENT) {
        // This is a safety query to ensure that we don't try to update the promotion status of the current version again
        // However, it's not thread-safe, so we'll check again below if we actaully updated anything
        log.debug("version $version of ${artifact.name} in env $targetEnvironment of app ${deliveryConfig.application} is already marked as deployed")
        false
      } else {
        val previouslyApprovedAt = getApprovedAt(deliveryConfig, artifact, version, targetEnvironment)
        jooq.transactionResult { config ->
          val txn = DSL.using(config)
          log.debug("markAsSuccessfullyDeployedTo: start transaction. name: ${artifact.name}. version: $version. env: $targetEnvironment")

          val deployedAt = clock.instant()

          val hasApprovedAtTime = previouslyApprovedAt != null
          val approvedAt = previouslyApprovedAt ?: deployedAt // Identical to deployedAt if missing

          if (!hasApprovedAtTime) {
            log.debug("setting approvedAt of version $version of artifact ${artifact.name} while marking it as deployed")
          }

          val current = txn.select(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS)
            .from(ENVIRONMENT_ARTIFACT_VERSIONS)
            .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(environmentUid))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(version))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.eq(CURRENT))
            .forUpdate()
            .fetchOneInto(PromotionStatus::class.java)

          if (current == CURRENT) {
            log.debug("version $version of ${artifact.name} in env $targetEnvironment of app ${deliveryConfig.application} was marked as deployed on another thread. Bailing...")
            return@transactionResult false
          }

          val currentUpdates = txn
            .insertInto(ENVIRONMENT_ARTIFACT_VERSIONS)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID, environmentUid)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID, artifact.uid)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION, version)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT, deployedAt)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, CURRENT)
            .onDuplicateKeyUpdate()
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, CURRENT)
            .execute()

          // We need to update the [deployedAt] and [approvedAt] separately cause otherwise the query above might return the wrong value
          txn.update(ENVIRONMENT_ARTIFACT_VERSIONS)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT, deployedAt)
            .apply {
              if (!hasApprovedAtTime) {
                set(ENVIRONMENT_ARTIFACT_VERSIONS.APPROVED_AT, approvedAt)
              }
            }
            .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(environmentUid))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(version))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.eq(CURRENT))
            .execute()


          recordEnvArtifactVersionEvent(deliveryConfig, environment, artifact, version, CURRENT, txn)

          // also insert into this table so the record for current doesn't get lost on a redeploy or a veto
          txn.insertInto(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS)
            .set(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.UID, randomUID().toString())
            .set(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID, environmentUid)
            .set(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID, artifactUid)
            .set(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION, version)
            .set(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT, deployedAt)
            .set(CURRENT_ENVIRONMENT_ARTIFACT_VERSIONS.JSON, "{}") //todo: add baseimage details here
            .execute()

          log.debug("markAsSuccessfullyDeployedTo: # of records marked CURRENT: $currentUpdates. name: ${artifact.name}. version: $version. env: $targetEnvironment")

          // update old "CURRENT" to "PREVIOUS
          val previousUpdates = txn
            .update(ENVIRONMENT_ARTIFACT_VERSIONS)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, PREVIOUS)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY, version)
            .set(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_AT, deployedAt)
            .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(environmentUid))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.eq(CURRENT))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.ne(version))
            .execute()

          log.debug("markAsSuccessfullyDeployedTo: # of records marked PREVIOUS: $previousUpdates. name: ${artifact.name}. version: $version. env: $targetEnvironment")
          // update any past artifacts that were "APPROVED" to be "SKIPPED"
          // because the new version takes precedence
          val approved = txn.selectArtifactVersionColumns()
            .from(ENVIRONMENT_ARTIFACT_VERSIONS, DELIVERY_ARTIFACT, ARTIFACT_VERSIONS)
            .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(environmentUid))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
            .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.eq(APPROVED))
            .and(DELIVERY_ARTIFACT.UID.eq(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID))
            .and(ARTIFACT_VERSIONS.NAME.eq(DELIVERY_ARTIFACT.NAME))
            .and(ARTIFACT_VERSIONS.TYPE.eq(DELIVERY_ARTIFACT.TYPE))
            .and(ARTIFACT_VERSIONS.VERSION.eq(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION))
            .fetchSortedArtifactVersions(artifact)
          log.debug("markAsSuccessfullyDeployedTo: # of records marked APPROVED: ${approved.size}. name: ${artifact.name}. version: $version. env: $targetEnvironment")

          val approvedButOld = approved
            .filter { isOlder(artifact, it, artifactVersion) }
            .map { it.version }
            .toTypedArray()

          log.debug("markAsSuccessfullyDeployedTo: # of approvedButOld: ${approvedButOld.size}. ${artifact.name}. version: $version. env: $targetEnvironment")

          if (approvedButOld.isNotEmpty()) {
            val skippedUpdates = txn
              .update(ENVIRONMENT_ARTIFACT_VERSIONS)
              .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, SKIPPED)
              .set(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY, version)
              .set(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_AT, clock.instant())
              .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(environmentUid))
              .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
              .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.eq(APPROVED))
              .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.`in`(*approvedButOld))
              .execute()
            batchRecordEnvArtifactVersionEvent(
              deliveryConfig,
              environment,
              artifact,
              approvedButOld.toList(),
              SKIPPED,
              txn
            )

            log.debug("markAsSuccessfullyDeployedTo: # of records marked SKIPPED: $skippedUpdates. name: ${artifact.name}. version: $version. env: $targetEnvironment")
          }

          val pendingButOld = getPendingVersionsInEnvironment(
            deliveryConfig,
            artifact.reference,
            targetEnvironment,
            txn
          ).filter { isOlder(artifact, it, artifactVersion) }
            .map { it.version }
            .take(100) // only take some of the records so this isn't a giant query
            .toTypedArray()

          log.debug("markAsSuccessfullyDeployedTo: # of pendingButOld: ${pendingButOld.size}. ${artifact.name}. version: $version. env: $targetEnvironment")

          if (pendingButOld.isNotEmpty()) {
            val skippedUpdates = txn
              .insertInto(
                ENVIRONMENT_ARTIFACT_VERSIONS,
                ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID,
                ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID,
                ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION,
                ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS,
                ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY,
                ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_AT
              )
              .apply {
                // this should skip all pending old versions in the env
                pendingButOld.forEach {
                  values(environmentUidString, artifactUid, it, SKIPPED, version, clock.instant())
                }
              }
              .execute()

            batchRecordEnvArtifactVersionEvent(
              deliveryConfig,
              environment,
              artifact,
              pendingButOld.toList(),
              SKIPPED,
              txn
            )
            log.debug("markAsSuccessfullyDeployedTo: # of pending versions marked SKIPPED: $skippedUpdates. name: ${artifact.name}. version: $version. env: $targetEnvironment")
          }
          addEnvAndArtifactForPostDeployAndVerifications(environmentUidString, artifactUid, version, txn)
          log.debug("markAsSuccessfullyDeployedTo complete. name: ${artifact.name}. version: $version. env: $targetEnvironment")
          return@transactionResult true
        }
      }
    }
  }

  /**
   * We need the environment uid / artifact uid combo to be present in the ENVIRONMENT_LAST_VERIFIED
   * and the ENVIRONMENT_LAST_POST_DEPLOY tables so that we check verifications and post deploy actions.
   */
  fun addEnvAndArtifactForPostDeployAndVerifications(
    envUid: String,
    artifactUid: String,
    version: String,
    txn: DSLContext
  ) {
      log.debug("markAsSuccessfullyDeployedTo: adding env + artifact uid to actions tables")
      txn
        .insertInto(ENVIRONMENT_LAST_VERIFIED)
        .set(ENVIRONMENT_LAST_VERIFIED.ENVIRONMENT_UID, envUid)
        .set(ENVIRONMENT_LAST_VERIFIED.ARTIFACT_UID, artifactUid)
        .set(ENVIRONMENT_LAST_VERIFIED.ARTIFACT_VERSION, version)
        .set(ENVIRONMENT_LAST_VERIFIED.AT, EPOCH.plusSeconds(1))
        .onDuplicateKeyUpdate() // we want a new version to be rechecked immediately
        .set(ENVIRONMENT_LAST_VERIFIED.ENVIRONMENT_UID, envUid)
        .set(ENVIRONMENT_LAST_VERIFIED.ARTIFACT_UID, artifactUid)
        .set(ENVIRONMENT_LAST_VERIFIED.ARTIFACT_VERSION, version)
        .set(ENVIRONMENT_LAST_VERIFIED.AT, EPOCH.plusSeconds(1))
        .execute()
      txn
        .insertInto(ENVIRONMENT_LAST_POST_DEPLOY)
        .set(ENVIRONMENT_LAST_POST_DEPLOY.ENVIRONMENT_UID, envUid)
        .set(ENVIRONMENT_LAST_POST_DEPLOY.ARTIFACT_UID, artifactUid)
        .set(ENVIRONMENT_LAST_POST_DEPLOY.ARTIFACT_VERSION, version)
        .set(ENVIRONMENT_LAST_POST_DEPLOY.AT, EPOCH.plusSeconds(1))
        .onDuplicateKeyUpdate() // we want a new version to be rechecked immediately
        .set(ENVIRONMENT_LAST_POST_DEPLOY.ENVIRONMENT_UID, envUid)
        .set(ENVIRONMENT_LAST_POST_DEPLOY.ARTIFACT_UID, artifactUid)
        .set(ENVIRONMENT_LAST_POST_DEPLOY.ARTIFACT_VERSION, version)
        .set(ENVIRONMENT_LAST_POST_DEPLOY.AT, EPOCH.plusSeconds(1))
        .execute()
  }

  override fun vetoedEnvironmentVersions(deliveryConfig: DeliveryConfig): List<EnvironmentArtifactVetoes> {
    val artifactsById = deliveryConfig.artifacts
      .associateBy { it.uidString }

    val vetoes: MutableMap<String, EnvironmentArtifactVetoes> = mutableMapOf()

    jooq.select(
      ENVIRONMENT.NAME,
      ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID,
      ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION,
      ENVIRONMENT_ARTIFACT_VETO.VETOED_BY,
      ENVIRONMENT_ARTIFACT_VETO.VETOED_AT,
      ENVIRONMENT_ARTIFACT_VETO.COMMENT,
    )
      .from(ENVIRONMENT)
      .innerJoin(ENVIRONMENT_ARTIFACT_VERSIONS)
      .on(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(ENVIRONMENT.UID))
      .innerJoin(ENVIRONMENT_ARTIFACT_VETO)
      .on(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(ENVIRONMENT_ARTIFACT_VETO.ENVIRONMENT_UID))
      .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(ENVIRONMENT_ARTIFACT_VETO.ARTIFACT_VERSION))
      .where(
        ENVIRONMENT.DELIVERY_CONFIG_UID.eq(deliveryConfig.uid),
        ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.eq(VETOED)
      )
      .fetch { (envName, artifactId, version, vetoedBy, vetoedAt, comment) ->
        if (artifactsById.containsKey(artifactId)) {
          vetoes.getOrPut(
            vetoKey(envName, artifactId)
          ) {
            EnvironmentArtifactVetoes(
              deliveryConfigName = deliveryConfig.name,
              targetEnvironment = envName,
              artifact = artifactsById[artifactId]
                ?: error("Invalid artifactId $artifactId for deliveryConfig ${deliveryConfig.name}"),
              versions = mutableSetOf()
            )
          }
            .versions.add(
              ArtifactVersionVetoData(
                version = version,
                vetoedAt = vetoedAt,
                vetoedBy = vetoedBy,
                comment = comment
              )
            )
        }
      }

    return vetoes.values.toList()
  }

  /**
   *  Record that an artifact version should be vetoed (marked as bad) in a target environment
   *
   *  @param deliveryConfig config object for looking up artifact information by artifact reference
   *  @param veto information about the artifact version to be vetoed and the target environment to veto in
   *  @param force if true, record the version as vetoed even if it is marked as the promotion reference
   *               (automated rollback target) of a previously vetoed version
   *
   *  @return true on success
   *
   * Preconditions for success:
   *
   *  1. [veto.version] is not currently pinned in the target environment
   *  2. One of the following is true:
   *     a. There is no record in the environment_artifacts_version table for this version
   *     b. The record in the environment_artifacts_versions table for this version has promotion_reference=NULL
   *     c. [force] is true
   *
   *  Note: 2b is a precondition to avoid cascading veto-triggered deployments. If R contains a promotion reference, then
   *  [veto.version] was originally deployed as a result of another artifact version being vetoed (see postcondition 3a).
   *
   *
   * Postconditions on success:
   *
   *  1. There exists a record R in the environment_artifacts_versions table such that:
   *     a. R references the artifact version and the target environment encoded in [veto]
   *     b. R.promotion_status="VETOED"
   *     c. If there exists a version that was previously deployed in [veto.targetEnvironment]:
   *       - R.promotion_reference=<prior deployed version>
   *     d. If there does not exist a version that was previously deployed in [veto.targetEnvironment]:
   *       - R.promotion_reference=[veto.version]
   *
   *  2. There exists a record T in the environment_artifacts_vetoes table such that:
   *      a. T references the the target environment and artifact version encoded in [veto]
   *
   *  3. If there exists a version that was previously deployed in [veto.targetEnironment]:
   *      a. There exists a record P in the environment_artifacts_versions table such that
   *        - P.artifact_version=<most recent previously deployed version>
   *        - P.promotion_reference=[veto.version]
   */
  override fun markAsVetoedIn(
    deliveryConfig: DeliveryConfig,
    veto: EnvironmentArtifactVeto,
    force: Boolean
  ): Boolean {
    val artifact = deliveryConfig.matchingArtifactByReference(veto.reference)
      ?: throw ArtifactNotFoundException(veto.reference, deliveryConfig.name)

    val (envUid, artUid) = environmentAndArtifactIds(deliveryConfig, veto.targetEnvironment, artifact)

    if (isPinned(envUid, artUid, veto.version)) {
      log.warn(
        "Pinned artifact version cannot be vetoed: " +
          "deliveryConfig=${deliveryConfig.name}, " +
          "environment=${veto.targetEnvironment}, " +
          "artifactVersion=${veto.version}"
      )
      return false
    }

    /**
     * If there's a promotion reference, that means this artifact version was deployed as a result of
     * another artifact version being vetoed. In that case, we don't veto unless [force] is enabled.
     */
    selectPromotionReference(envUid, artUid, veto.version)
      .fetchOne(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_REFERENCE)
      ?.let { reference ->
        if (!force) {
          log.warn(
            "Not vetoing artifact version as it appears to have already been an automated rollback target: " +
              "deliveryConfig=${deliveryConfig.name}, " +
              "environment=${veto.targetEnvironment}, " +
              "artifactVersion=${veto.version}, " +
              "priorVersionReference=$reference"
          )
          return false
        }
      }

    val prior = priorVersionDeployedIn(envUid, artUid, veto.version)

    sqlRetry.withRetry(WRITE) {
      jooq.transaction { config ->
        val txn = DSL.using(config)
        txn.upsertAsVetoedInEnvironmentArtifactVersionsTable(prior, veto, envUid, artUid)
        txn.addRecordToEnvironmentArtifactVetoTable(envUid, artUid, veto)
        recordEnvArtifactVersionEvent(deliveryConfig, deliveryConfig.environmentNamed(veto.targetEnvironment), artifact, veto.version, VETOED, txn)

        /**
         * If there's a previously deployed version in [targetEnvironment], set `promotion_reference`
         * to the version that's currently being vetoed. If that version also fails to fully deploy,
         * this is used to short-circuit further automated vetoes. We want to avoid a cloud provider
         * or other issue unrelated to an artifact version triggering continual automated rollbacks
         * thru all previously deployed versions.
         */
        prior?.let { txn.setPromotionReference(veto.version, envUid, artUid, it) }
      }
    }

    return true
  }

  private fun DSLContext.setPromotionReference(version: String, envUid: String, artUid: String, prior: String) {
    update(ENVIRONMENT_ARTIFACT_VERSIONS)
      .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_REFERENCE, version)
      .where(
        ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(envUid),
        ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artUid),
        ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(prior)
      )
      .execute()
  }

  private fun DSLContext.addRecordToEnvironmentArtifactVetoTable(
    envUid: String,
    artUid: String,
    veto: EnvironmentArtifactVeto
  ) {
    insertInto(ENVIRONMENT_ARTIFACT_VETO)
      .set(ENVIRONMENT_ARTIFACT_VETO.ENVIRONMENT_UID, envUid)
      .set(ENVIRONMENT_ARTIFACT_VETO.ARTIFACT_UID, artUid)
      .set(ENVIRONMENT_ARTIFACT_VETO.ARTIFACT_VERSION, veto.version)
      .set(ENVIRONMENT_ARTIFACT_VETO.VETOED_AT, clock.instant())
      .set(ENVIRONMENT_ARTIFACT_VETO.VETOED_BY, veto.vetoedBy)
      .set(ENVIRONMENT_ARTIFACT_VETO.COMMENT, veto.comment)
      .onDuplicateKeyUpdate()
      .set(ENVIRONMENT_ARTIFACT_VETO.VETOED_AT, clock.instant())
      .set(ENVIRONMENT_ARTIFACT_VETO.VETOED_BY, veto.vetoedBy)
      .set(ENVIRONMENT_ARTIFACT_VETO.COMMENT, veto.comment)
      .execute()
  }

  private fun DSLContext.upsertAsVetoedInEnvironmentArtifactVersionsTable(
    prior: String?,
    veto: EnvironmentArtifactVeto,
    envUid: String,
    artUid: String
  ) {
    insertInto(ENVIRONMENT_ARTIFACT_VERSIONS)
      .set(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID, envUid)
      .set(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID, artUid)
      .set(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION, veto.version)
      .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, VETOED)
      .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_REFERENCE, prior ?: veto.version)
      .onDuplicateKeyUpdate()
      .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, VETOED)
      .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_REFERENCE, prior ?: veto.version)
      .execute()
  }

  private fun selectPromotionReference(
    envUid: String,
    artUid: String,
    version: String
  ): SelectConditionStep<Record1<String>> {
    return jooq
      .select(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_REFERENCE)
      .from(ENVIRONMENT_ARTIFACT_VERSIONS)
      .where(
        ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(envUid),
        ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artUid),
        ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(version)
      )
  }

  private fun isPinned(
    envUid: String,
    artUid: String,
    version: String
  ): Boolean {
    return sqlRetry.withRetry(READ) {
      jooq
        .fetchExists(
          ENVIRONMENT_ARTIFACT_PIN,
          ENVIRONMENT_ARTIFACT_PIN.ENVIRONMENT_UID.eq(envUid)
            .and(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_UID.eq(artUid))
            .and(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_VERSION.eq(version))
        )
    }
  }

  private fun environmentAndArtifactIds(
    deliveryConfig: DeliveryConfig,
    targetEnvironment: String,
    artifact: DeliveryArtifact
  ): Pair<String, String> {
    return sqlRetry.withRetry(READ) {
      Pair(
        deliveryConfig.getUidStringFor(
          deliveryConfig.environmentNamed(targetEnvironment)
        ),
        artifact.uidString
      )
    }
  }

  override fun deleteVeto(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String
  ) {
    val envId = deliveryConfig.getUidFor(
      deliveryConfig.environmentNamed(targetEnvironment)
    )
    val artId = artifact.uidString

    sqlRetry.withRetry(WRITE) {
      val referenceVersion: String? = jooq.select(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_REFERENCE)
        .from(ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(
          ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(envId),
          ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid),
          ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(version)
        )
        .fetchOne(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_REFERENCE)

      /**
       * If there are bidirectional `PROMOTION_REFERENCE` markers between this [version]
       * and another (i.e. the veto was applied in order to rollback from this version
       * to the other), both sides are removed.
       */
      val referencesReferenceVersion: String? = when (referenceVersion) {
        null -> null
        else -> {
          jooq.select(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_REFERENCE)
            .from(ENVIRONMENT_ARTIFACT_VERSIONS)
            .where(
              ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(envId),
              ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artId),
              ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(referenceVersion)
            )
            .fetchOne(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_REFERENCE)
        }
      }

      val status: PromotionStatus = jooq
        .select(
          ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT,
          ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY
        )
        .from(ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(
          ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(envId),
          ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artId),
          ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(version)
        )
        .fetchSingle { (deployedAt, replacedBy) ->
          if (deployedAt != null && replacedBy != null) {
            PREVIOUS
          } else if (deployedAt != null) {
            CURRENT
          } else {
            APPROVED
          }
        }

      jooq.transaction { config ->
        val txn = DSL.using(config)

        txn.update(ENVIRONMENT_ARTIFACT_VERSIONS)
          .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, status)
          .setNull(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_REFERENCE)
          .where(
            ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(envId),
            ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artId),
            ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(version)
          )
          .execute()

        if (referencesReferenceVersion != null && referenceVersion == referencesReferenceVersion) {
          txn.update(ENVIRONMENT_ARTIFACT_VERSIONS)
            .setNull(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_REFERENCE)
            .where(
              ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(envId),
              ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artId),
              ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(referenceVersion)
            )
            .execute()
        }

        txn.deleteFrom(ENVIRONMENT_ARTIFACT_VETO)
          .where(ENVIRONMENT_ARTIFACT_VETO.ENVIRONMENT_UID.eq(envId))
          .and(ENVIRONMENT_ARTIFACT_VETO.ARTIFACT_UID.eq(artId))
          .and(ENVIRONMENT_ARTIFACT_VETO.ARTIFACT_VERSION.eq(version))
          .execute()
      }
    }
  }

  override fun markAsSkipped(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String,
    supersededByVersion: String?
  ) {
    val environment = deliveryConfig.environmentNamed(targetEnvironment)
    val environmentUid = deliveryConfig.getUidFor(environment)
    sqlRetry.withRetry(WRITE) {
      jooq
        .insertInto(ENVIRONMENT_ARTIFACT_VERSIONS)
        .set(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID, environmentUid)
        .set(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID, artifact.uid)
        .set(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION, version)
        .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, SKIPPED)
        .set(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY, supersededByVersion)
        .set(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_AT, clock.instant())
        .onDuplicateKeyUpdate()
        .set(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS, SKIPPED)
        .set(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY, supersededByVersion)
        .set(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_AT, clock.instant())
        .execute()

      recordEnvArtifactVersionEvent(deliveryConfig, environment, artifact, version, SKIPPED, jooq)
    }
  }

  override fun getArtifactVersionsByStatus(
    deliveryConfig: DeliveryConfig,
    artifactReference: String,
    environmentName: String,
    statuses: List<PromotionStatus>
  ): List<PublishedArtifact> {
    return sqlRetry.withRetry(READ) {
      jooq
        .select(
          ARTIFACT_VERSIONS.NAME,
          ARTIFACT_VERSIONS.TYPE,
          ARTIFACT_VERSIONS.VERSION,
          DELIVERY_ARTIFACT.REFERENCE,
          ARTIFACT_VERSIONS.RELEASE_STATUS,
          ARTIFACT_VERSIONS.CREATED_AT,
          ARTIFACT_VERSIONS.GIT_METADATA,
          ARTIFACT_VERSIONS.BUILD_METADATA,
          ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS
        )
        .from(ENVIRONMENT_ARTIFACT_VERSIONS)
        .innerJoin(DELIVERY_ARTIFACT)
        .on(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(DELIVERY_ARTIFACT.UID))
        .innerJoin(ARTIFACT_VERSIONS)
        .on(DELIVERY_ARTIFACT.NAME.eq(ARTIFACT_VERSIONS.NAME))
        .and(DELIVERY_ARTIFACT.TYPE.eq(ARTIFACT_VERSIONS.TYPE))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(ARTIFACT_VERSIONS.VERSION))
        .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(deliveryConfig.getUidFor(environmentName)))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.`in`(statuses))
        .and(DELIVERY_ARTIFACT.REFERENCE.eq(artifactReference))
        .fetch { (name, type, version, reference, status, createdAt, gitMetadata, buildMetadata, promotionStatus) ->
          PublishedArtifact(
            name = name,
            type = type,
            version = version,
            reference = reference,
            status = status,
            createdAt = createdAt,
            gitMetadata = gitMetadata,
            buildMetadata = buildMetadata,
            statusInEnvironment = promotionStatus.name
          )
        }
    }
  }

  @NewSpan
  override fun getAllVersionsForEnvironment(
    artifact: DeliveryArtifact,
    config: DeliveryConfig,
    environmentName: String
  ): List<PublishedArtifactInEnvironment> {
    var existingVersions: List<PublishedArtifactInEnvironment> = sqlRetry.withRetry(READ) {
      jooq
        .select(
          ARTIFACT_VERSIONS.NAME,
          ARTIFACT_VERSIONS.TYPE,
          ARTIFACT_VERSIONS.VERSION,
          DELIVERY_ARTIFACT.REFERENCE,
          ARTIFACT_VERSIONS.RELEASE_STATUS,
          ARTIFACT_VERSIONS.CREATED_AT,
          ARTIFACT_VERSIONS.GIT_METADATA,
          ARTIFACT_VERSIONS.BUILD_METADATA,
          ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS,
          ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT,
          ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY,
          ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_AT,
        )
        .from(ENVIRONMENT_ARTIFACT_VERSIONS)
        .innerJoin(DELIVERY_ARTIFACT)
        .on(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(DELIVERY_ARTIFACT.UID))
        .innerJoin(ARTIFACT_VERSIONS)
        .on(DELIVERY_ARTIFACT.NAME.eq(ARTIFACT_VERSIONS.NAME))
        .innerJoin(ACTIVE_ENVIRONMENT)
        .on(ACTIVE_ENVIRONMENT.UID.eq(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID))
        .and(DELIVERY_ARTIFACT.TYPE.eq(ARTIFACT_VERSIONS.TYPE))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(ARTIFACT_VERSIONS.VERSION))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
        .where(ACTIVE_ENVIRONMENT.NAME.eq(environmentName))
        .fetch { (name, type, version, reference, status, createdAt, gitMetadata, buildMetadata, promotionStatus, deployedAt, replacedBy, replacedAt) ->
          val publishedArtifact = PublishedArtifact(
            name = name,
            type = type,
            version = version,
            reference = reference,
            status = status,
            createdAt = createdAt,
            gitMetadata = gitMetadata,
            buildMetadata = buildMetadata,
          )
          PublishedArtifactInEnvironment(
            publishedArtifact,
            promotionStatus,
            environmentName,
            deployedAt,
            replacedBy,
            replacedAt
          )
        }
    }
    val pending = getPendingVersions(artifact, config, environmentName)
    val current = getCurrentPublishedArtifactInEnvironment(config, artifact, environmentName)
    if (current != null) {
      existingVersions = existingVersions.map {
        if (it.publishedArtifact.version == current.publishedArtifact.version && it.publishedArtifact.type == current.publishedArtifact.type) {
          it.copy(isCurrent = true)
        } else {
          it
        }
      }
    }
    return existingVersions + pending
  }

  private fun getCurrentPublishedArtifactInEnvironment(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    environmentName: String
  ): PublishedArtifactInEnvironment? {
    getCurrentlyDeployedArtifactVersion(deliveryConfig, artifact, environmentName)?.let { publishedArtifact ->
      return PublishedArtifactInEnvironment(
        publishedArtifact,
        CURRENT,
        environmentName,
        publishedArtifact.metadata["deployedAt"] as? Instant,
        null
      )
    }
    return null
  }

  /**
   * Gets all pending version for each environment
   */
  private fun getPendingVersions(
    artifact: DeliveryArtifact,
    config: DeliveryConfig,
    environmentName: String
  ): List<PublishedArtifactInEnvironment> =
    getPendingVersionsInEnvironment(config, artifact.reference, environmentName)
      .map { publishedArtifact ->
        PublishedArtifactInEnvironment(
          publishedArtifact,
          PENDING,
          environmentName
        )
      }

  /**
   * Common select criteria for finding artifacts that belong with a delivery config and environment.
   */
  private fun <T : Record?> SelectJoinStep<T>.whereArtifactVersionsMatch(deliveryConfig: DeliveryConfig, environment: Environment, artifact: DeliveryArtifact) =
    where(DELIVERY_ARTIFACT.NAME.eq(artifact.name))
      .and(DELIVERY_ARTIFACT.TYPE.eq(artifact.type))
      .and(DELIVERY_ARTIFACT.REFERENCE.eq(artifact.reference))
      .and(DELIVERY_ARTIFACT.DELIVERY_CONFIG_NAME.eq(deliveryConfig.name))
      .and(DELIVERY_CONFIG.NAME.eq(deliveryConfig.name))
      .and(ACTIVE_ENVIRONMENT.DELIVERY_CONFIG_UID.eq(DELIVERY_CONFIG.UID))
      .and(ACTIVE_ENVIRONMENT.NAME.eq(environment.name))
      .and(ARTIFACT_VERSIONS.NAME.eq(artifact.name))
      .and(ARTIFACT_VERSIONS.TYPE.eq(artifact.type))
      .apply {
        if (artifact.statuses.isNotEmpty()) {
          and(ARTIFACT_VERSIONS.RELEASE_STATUS.`in`(*artifact.statuses.toTypedArray()))
        }
        // For preview environments, select only those artifact versions with a matching branch
        if (environment.isPreview) {
          and(ARTIFACT_VERSIONS.BRANCH.eq(ACTIVE_ENVIRONMENT.BRANCH))
        }
      }

  override fun getPendingVersionsInEnvironment(
    deliveryConfig: DeliveryConfig,
    artifactReference: String,
    environmentName: String
  ): List<PublishedArtifact> {
    return sqlRetry.withRetry(READ) {
      jooq.transactionResult { config ->
        val txn = DSL.using(config)
        getPendingVersionsInEnvironment(deliveryConfig, artifactReference, environmentName, txn)
      }
    }
  }

  private fun getPendingVersionsInEnvironment(
    deliveryConfig: DeliveryConfig,
    artifactReference: String,
    environmentName: String,
    txn: DSLContext
  ): List<PublishedArtifact> {
    val artifact = deliveryConfig.matchingArtifactByReference(artifactReference)
      ?: throw ArtifactNotFoundException(artifactReference, deliveryConfig.name)
    val environment = deliveryConfig.environmentNamed(environmentName)
    return txn
      .selectArtifactVersionColumns()
      .from(
        ARTIFACT_VERSIONS,
        DELIVERY_ARTIFACT,
        ACTIVE_ENVIRONMENT,
        DELIVERY_CONFIG
      )
      .whereArtifactVersionsMatch(deliveryConfig, environment, artifact)
      .andNotExists(
        selectOne()
          .from(ENVIRONMENT_ARTIFACT_VERSIONS)
          .where(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(ARTIFACT_VERSIONS.VERSION))
          .and(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(ACTIVE_ENVIRONMENT.UID))
          .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(DELIVERY_ARTIFACT.UID))
      )
      .fetchArtifactVersions(artifactReference)
      .filter {
        artifact.hasMatchingSource(it.gitMetadata)
      }
  }

  override fun getNotYetDeployedVersionsInEnvironment(
    deliveryConfig: DeliveryConfig,
    artifactReference: String,
    environmentName: String
  ): List<PublishedArtifact> {
    val pending = getPendingVersionsInEnvironment(deliveryConfig, artifactReference, environmentName)
    val skipped = getArtifactVersionsByStatus(deliveryConfig, artifactReference, environmentName, listOf(SKIPPED))
    return pending + skipped
  }

  override fun getNumPendingToBePromoted(
    deliveryConfig: DeliveryConfig,
    artifactReference: String,
    environmentName: String,
    version: String
  ): Int {
    val artifact = deliveryConfig.matchingArtifactByReference(artifactReference)
      ?: throw ArtifactNotFoundException(artifactReference, deliveryConfig.name)
    val pendingVersions = getPendingVersions(artifact, deliveryConfig, environmentName)
      .map { it.publishedArtifact }

    val currentVersion = getCurrentlyDeployedArtifactVersion(
      deliveryConfig,
      artifact,
      environmentName
    )
    return removeExtra(pendingVersions, artifact, version, currentVersion).size
  }

  /**
   * Removes any pending artifacts that are newer than the specified version, because they would not be promoted.
   * Removes any older than the current version, because they are not relevant.
   */
  fun removeExtra(versions: List<PublishedArtifact>, artifact: DeliveryArtifact, mjVersion: String, currentVersion: PublishedArtifact?): List<PublishedArtifact> {
    if (versions.isEmpty()) {
      return emptyList()
    }
    val fullVersions = if (currentVersion != null) {
      versions + currentVersion
    } else {
      versions
    }

    val sortedVersions = fullVersions.sortedWith(artifact.sortingStrategy.comparator) // newest will be first
    val mjArtifact = sortedVersions.find { it.version == mjVersion }
      ?: return sortedVersions // mj version isn't still pending? or there is a bug?
    // remove all versions that are newer than the mj version, they won't be promoted
    val newerRemoved = sortedVersions.dropWhile { it != mjArtifact }

    return if (currentVersion != null) {
      // remove all versions that are older than the current version, and the current version
      newerRemoved.dropLastWhile { it != currentVersion }.filterNot { it == currentVersion }
    } else {
      newerRemoved
    }
  }

  override fun getEnvironmentSummaries(deliveryConfig: DeliveryConfig): List<EnvironmentSummary> {
    val pinnedEnvs = getPinnedEnvironments(deliveryConfig)
    return deliveryConfig.environments.map { environment ->
      val artifactVersions = deliveryConfig.artifacts.map { artifact ->
        val versionsInEnvironment = jooq
          .select(
            ARTIFACT_VERSIONS.NAME,
            ARTIFACT_VERSIONS.TYPE,
            ARTIFACT_VERSIONS.VERSION,
            ARTIFACT_VERSIONS.RELEASE_STATUS,
            ARTIFACT_VERSIONS.CREATED_AT,
            ARTIFACT_VERSIONS.GIT_METADATA,
            ARTIFACT_VERSIONS.BUILD_METADATA,
            ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS
          )
          .from(
            ENVIRONMENT_ARTIFACT_VERSIONS,
            ARTIFACT_VERSIONS,
            DELIVERY_ARTIFACT,
            ACTIVE_ENVIRONMENT,
            DELIVERY_CONFIG
          )
          .whereArtifactVersionsMatch(deliveryConfig, environment, artifact)
          .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(DELIVERY_ARTIFACT.UID))
          .and(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(ACTIVE_ENVIRONMENT.UID))
          .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(ARTIFACT_VERSIONS.VERSION))

        val pendingVersions = jooq
          .select(
            ARTIFACT_VERSIONS.NAME,
            ARTIFACT_VERSIONS.TYPE,
            ARTIFACT_VERSIONS.VERSION,
            ARTIFACT_VERSIONS.RELEASE_STATUS,
            ARTIFACT_VERSIONS.CREATED_AT,
            ARTIFACT_VERSIONS.GIT_METADATA,
            ARTIFACT_VERSIONS.BUILD_METADATA,
            DSL.`val`(PENDING)
          )
          .from(
            ARTIFACT_VERSIONS,
            DELIVERY_ARTIFACT,
            ACTIVE_ENVIRONMENT,
            DELIVERY_CONFIG
          )
          .whereArtifactVersionsMatch(deliveryConfig, environment, artifact)
          .andNotExists(
            selectOne()
              .from(ENVIRONMENT_ARTIFACT_VERSIONS)
              .where(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(ARTIFACT_VERSIONS.VERSION))
              .and(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(ACTIVE_ENVIRONMENT.UID))
              .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(DELIVERY_ARTIFACT.UID))
          )

        val unionedVersions = sqlRetry.withRetry(READ) {
          versionsInEnvironment
            .unionAll(pendingVersions)
            .fetch { (name, type, version, status, createdAt, gitMetadata, buildMetadata, promotionStatus) ->
              PublishedArtifact(
                name = name,
                type = type,
                version = version,
                status = status,
                createdAt = createdAt,
                gitMetadata = gitMetadata,
                buildMetadata = buildMetadata,
              ) to promotionStatus
            }
        }
          .filter { (artifactVersion, _) ->
            if (artifact is DockerArtifact) {
              // filter out invalid docker tags
              filterDockerTag(artifactVersion.version, artifact)
            } else {
              true
            }
          }

        val releaseStatuses: Set<ArtifactStatus> = unionedVersions
          .mapNotNull { (artifactVersion, _) ->
            artifactVersion.status
          }
          .toSet()

        val versions = unionedVersions
          .sortedWith(compareBy(artifact.sortingStrategy.comparator) { (artifactVersion, _) -> artifactVersion })
          .groupBy(
            { (_, promotionStatus) ->
              promotionStatus
            },
            { (artifactVersion, _) ->
              artifactVersion
            }
          )

        val currentVersion = getCurrentlyDeployedArtifactVersion(deliveryConfig, artifact, environment.name)
        ArtifactVersions(
          name = artifact.name,
          type = artifact.type,
          reference = artifact.reference,
          statuses = releaseStatuses,
          versions = ArtifactVersionStatus(
            current = currentVersion?.version,
            deploying = versions[DEPLOYING]?.firstOrNull()?.version,
            // take out stateful constraint values that will never happen
            pending = removeOlderIfCurrentExists(artifact, currentVersion, versions[PENDING]).map { it.version },
            approved = (versions[APPROVED] ?: emptyList()).map { it.version },
            previous = (versions[PREVIOUS] ?: emptyList()).map { it.version },
            vetoed = (versions[VETOED] ?: emptyList()).map { it.version },
            skipped = removeNewerIfCurrentExists(artifact, currentVersion, versions[PENDING])
              .plus(versions[SKIPPED] ?: emptyList()).map { it.version }
          ),
          pinnedVersion = pinnedEnvs.find { it.targetEnvironment == environment.name }?.version
        )
      }.toSet()
      EnvironmentSummary(environment, artifactVersions)
    }
  }

  override fun pinEnvironment(deliveryConfig: DeliveryConfig, environmentArtifactPin: EnvironmentArtifactPin) {
    with(environmentArtifactPin) {
      val environment = deliveryConfig.environmentNamed(targetEnvironment)
      val artifact = get(deliveryConfig.name, reference)
      val now = clock.instant()
      sqlRetry.withRetry(WRITE) {
        jooq.insertInto(ENVIRONMENT_ARTIFACT_PIN)
          .set(ENVIRONMENT_ARTIFACT_PIN.ENVIRONMENT_UID, deliveryConfig.getUidFor(environment))
          .set(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_UID, artifact.uid)
          .set(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_VERSION, version)
          .set(ENVIRONMENT_ARTIFACT_PIN.PINNED_AT, now)
          .set(ENVIRONMENT_ARTIFACT_PIN.PINNED_BY, pinnedBy ?: "anonymous")
          .set(ENVIRONMENT_ARTIFACT_PIN.COMMENT, comment)
          .set(ENVIRONMENT_ARTIFACT_PIN.TYPE, type)
          .onDuplicateKeyUpdate()
          .set(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_VERSION, version)
          .set(ENVIRONMENT_ARTIFACT_PIN.PINNED_AT, now)
          .set(ENVIRONMENT_ARTIFACT_PIN.PINNED_BY, pinnedBy ?: "anonymous")
          .set(ENVIRONMENT_ARTIFACT_PIN.COMMENT, MySQLDSL.values(ENVIRONMENT_ARTIFACT_PIN.COMMENT))
          .set(ENVIRONMENT_ARTIFACT_PIN.TYPE, type)
          .execute()
      }
    }
  }

  override fun getPinnedEnvironments(deliveryConfig: DeliveryConfig): List<PinnedEnvironment> {
    return sqlRetry.withRetry(READ) {
      jooq.select(
        ENVIRONMENT.NAME,
        ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_VERSION,
        ENVIRONMENT_ARTIFACT_PIN.PINNED_AT,
        ENVIRONMENT_ARTIFACT_PIN.PINNED_BY,
        ENVIRONMENT_ARTIFACT_PIN.COMMENT,
        ENVIRONMENT_ARTIFACT_PIN.TYPE,
        DELIVERY_ARTIFACT.NAME,
        DELIVERY_ARTIFACT.TYPE,
        DELIVERY_ARTIFACT.DETAILS,
        DELIVERY_ARTIFACT.REFERENCE,
        DELIVERY_ARTIFACT.IS_PREVIEW
      )
        .from(ENVIRONMENT)
        .innerJoin(ENVIRONMENT_ARTIFACT_PIN)
        .on(ENVIRONMENT_ARTIFACT_PIN.ENVIRONMENT_UID.eq(ENVIRONMENT.UID))
        .innerJoin(DELIVERY_ARTIFACT)
        .on(DELIVERY_ARTIFACT.UID.eq(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_UID))
        .innerJoin(DELIVERY_CONFIG)
        .on(DELIVERY_CONFIG.UID.eq(ENVIRONMENT.DELIVERY_CONFIG_UID))
        .where(DELIVERY_CONFIG.NAME.eq(deliveryConfig.name))
        .fetch { (environmentName, version, pinnedAt, pinnedBy, comment, pinType, artifactName, type, details, reference, isPreview) ->
          PinnedEnvironment(
            deliveryConfigName = deliveryConfig.name,
            targetEnvironment = environmentName,
            artifact = objectMapper.mapToArtifact(
              artifactSuppliers.supporting(type),
              artifactName,
              type.lowercase(),
              details,
              reference,
              deliveryConfig.name,
              isPreview
            ),
            version = version,
            pinnedAt = pinnedAt,
            pinnedBy = pinnedBy,
            comment = comment,
            type = pinType
          )
        }
    }
  }

  override fun deletePin(deliveryConfig: DeliveryConfig, targetEnvironment: String) {
    sqlRetry.withRetry(WRITE) {
      jooq.select(ENVIRONMENT_ARTIFACT_PIN.ENVIRONMENT_UID, ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_UID)
        .from(ENVIRONMENT_ARTIFACT_PIN)
        .innerJoin(ENVIRONMENT)
        .on(ENVIRONMENT_ARTIFACT_PIN.ENVIRONMENT_UID.eq(ENVIRONMENT.UID))
        .where(
          ENVIRONMENT.NAME.eq(targetEnvironment),
          ENVIRONMENT.DELIVERY_CONFIG_UID.eq(deliveryConfig.uid)
        )
        .fetch { (envUid, artUid) ->
          deletePin(envUid, artUid)
        }
    }
  }

  override fun getPinnedVersion(
    deliveryConfig: DeliveryConfig,
    targetEnvironment: String,
    reference: String
  ): String? {
    return sqlRetry.withRetry(READ) {
      jooq.select(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_VERSION)
        .from(DELIVERY_ARTIFACT)
        .innerJoin(ENVIRONMENT_ARTIFACT_PIN)
        .on(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_UID.eq(DELIVERY_ARTIFACT.UID))
        .innerJoin(ENVIRONMENT)
        .on(ENVIRONMENT_ARTIFACT_PIN.ENVIRONMENT_UID.eq(ENVIRONMENT.UID))
        .where(
          DELIVERY_ARTIFACT.DELIVERY_CONFIG_NAME.eq(deliveryConfig.name),
          DELIVERY_ARTIFACT.REFERENCE.eq(reference),
          ENVIRONMENT.NAME.eq(targetEnvironment),
          ENVIRONMENT.DELIVERY_CONFIG_UID.eq(deliveryConfig.uid)
        )
        .fetch(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_VERSION)
        .firstOrNull()
    }
  }

  override fun deletePin(
    deliveryConfig: DeliveryConfig,
    targetEnvironment: String,
    reference: String
  ) {
    sqlRetry.withRetry(WRITE) {
      jooq.select(ENVIRONMENT_ARTIFACT_PIN.ENVIRONMENT_UID, ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_UID)
        .from(DELIVERY_ARTIFACT)
        .innerJoin(ENVIRONMENT_ARTIFACT_PIN)
        .on(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_UID.eq(DELIVERY_ARTIFACT.UID))
        .innerJoin(ENVIRONMENT)
        .on(ENVIRONMENT_ARTIFACT_PIN.ENVIRONMENT_UID.eq(ENVIRONMENT.UID))
        .where(
          DELIVERY_ARTIFACT.DELIVERY_CONFIG_NAME.eq(deliveryConfig.name),
          DELIVERY_ARTIFACT.REFERENCE.eq(reference),
          ENVIRONMENT.NAME.eq(targetEnvironment),
          ENVIRONMENT.DELIVERY_CONFIG_UID.eq(deliveryConfig.uid)
        )
        .fetch { (envUid, artUid) ->
          deletePin(envUid, artUid)
        }
    }
  }

  override fun getArtifactVersionByPromotionStatus(
    deliveryConfig: DeliveryConfig,
    environmentName: String,
    artifact: DeliveryArtifact,
    promotionStatus: PromotionStatus,
    version: String?
  ): PublishedArtifact? {
    //only CURRENT and PREVIOUS are supported, as they can be sorted by deploy_at
    require(promotionStatus in listOf(CURRENT, PREVIOUS)) { "Invalid promotion status used to query" }

    return sqlRetry.withRetry(READ) {
      val fetchedVersion = jooq
        .select(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION)
        .from(ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(deliveryConfig.getUidFor(environmentName)))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.eq(promotionStatus))
        //special case for pinning
        .apply { if (version != null) and(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY.eq(version)) }
        .orderBy(ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT.desc())
        .fetch(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION)
        .firstOrNull()

      if (fetchedVersion != null) {
        getArtifactVersion(artifact, fetchedVersion)
      } else {
        null
      }
    }
  }

  override fun getVersionInfoInEnvironment(
    deliveryConfig: DeliveryConfig,
    environmentName: String,
    artifact: DeliveryArtifact
  ): List<StatusInfoForArtifactInEnvironment> =
    sqlRetry.withRetry(READ) {
      jooq
        .select(
          ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION,
          ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS,
          ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT,
          ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY
        )
        .from(ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(deliveryConfig.getUidFor(environmentName)))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.`in`(listOf(CURRENT, PREVIOUS)))
        .orderBy(ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT.desc())
        .fetch { (version, status, deployedAt, replacedBy) ->
          StatusInfoForArtifactInEnvironment(
            version,
            status,
            replacedBy,
            deployedAt
          )
        }
    }

  /**
   * Returns summary information for the provided list of versions.
   */
  override fun getArtifactSummariesInEnvironment(
    deliveryConfig: DeliveryConfig,
    environmentName: String,
    artifactReference: String,
    versions: List<String>
  ): List<ArtifactSummaryInEnvironment> {
    val artifact = deliveryConfig.artifacts.firstOrNull { it.reference == artifactReference }
      ?: error("Artifact not found: name=$artifactReference, deliveryConfig=${deliveryConfig.name}")

    // only one version can be pinned
    val pinned: Pair<String, ActionMetadata>? = sqlRetry.withRetry(READ) { //version, pinned by info
      jooq.select(
        ENVIRONMENT_ARTIFACT_PIN.PINNED_BY,
        ENVIRONMENT_ARTIFACT_PIN.PINNED_AT,
        ENVIRONMENT_ARTIFACT_PIN.COMMENT,
        ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_VERSION
      )
        .from(ENVIRONMENT_ARTIFACT_PIN)
        .where(ENVIRONMENT_ARTIFACT_PIN.ENVIRONMENT_UID.eq(deliveryConfig.getUidFor(environmentName)))
        .and(ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_UID.eq(artifact.uid))
        .fetchOne { (pinnedBy, pinnedAt, comment, version) ->
          Pair(version, ActionMetadata(at = pinnedAt, by = pinnedBy, comment = comment))
        }
    }

    val vetoed: Map<String, ActionMetadata> = sqlRetry.withRetry(READ) {
      jooq.select(
        ENVIRONMENT_ARTIFACT_VETO.VETOED_AT,
        ENVIRONMENT_ARTIFACT_VETO.VETOED_BY,
        ENVIRONMENT_ARTIFACT_VETO.COMMENT,
        ENVIRONMENT_ARTIFACT_VETO.ARTIFACT_VERSION
      )
        .from(ENVIRONMENT_ARTIFACT_VETO)
        .where(ENVIRONMENT_ARTIFACT_VETO.ENVIRONMENT_UID.eq(deliveryConfig.getUidFor(environmentName)))
        .and(ENVIRONMENT_ARTIFACT_VETO.ARTIFACT_UID.eq(artifact.uid))
        .fetch { (vetoedAt, vetoedBy, comment, version) ->
          version to ActionMetadata(at = vetoedAt, by = vetoedBy, comment = comment)
        }.toMap()
    }

    return sqlRetry.withRetry(READ) {
      jooq.select(
        ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION,
        ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT,
        ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS,
        ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY,
        ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_AT
      )
        .from(ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(deliveryConfig.getUidFor(environmentName)))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.`in`(versions))
        .orderBy(ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT.desc())
        .fetch { (version, deployedAt, promotionStatus, replacedBy, replacedAt) ->
          ArtifactSummaryInEnvironment(
            environment = environmentName,
            version = version,
            state = promotionStatus.name.lowercase(),
            deployedAt = deployedAt,
            replacedAt = replacedAt,
            replacedBy = replacedBy,
            pinned = if (pinned != null && pinned.first == version) pinned.second else null,
            vetoed = vetoed[version]
          )
        }
    }
  }

  override fun getArtifactPromotionStatus(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    environmentName: String
  ): PromotionStatus? {
    return sqlRetry.withRetry(READ) {
      jooq
        .select(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS)
        .from(ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(deliveryConfig.getUidFor(environmentName)))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(version))
        .fetchOne(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS)
    }
  }

  override fun getDeployedAt(
    deliveryConfig: DeliveryConfig,
    environment: Environment,
    artifact: DeliveryArtifact,
    version: String
  ): Instant {
    return sqlRetry.withRetry(READ) {
      jooq
        .select(ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT)
        .from(ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(deliveryConfig.getUidFor(environment.name)))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifact.uid))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(version))
        .fetchOne(ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT)
        ?: throw NoSuchDeploymentException(deliveryConfig, artifact, environment, version)
    }
  }

  override fun itemsDueForCheck(minTimeSinceLastCheck: Duration, limit: Int): Collection<DeliveryArtifact> {
    val now = clock.instant()
    val cutoff = now.minus(minTimeSinceLastCheck)
    return sqlRetry.withRetry(WRITE) {
      jooq.inTransaction {
        select(
          DELIVERY_ARTIFACT.UID,
          DELIVERY_ARTIFACT.NAME,
          DELIVERY_ARTIFACT.TYPE,
          DELIVERY_ARTIFACT.DETAILS,
          DELIVERY_ARTIFACT.REFERENCE,
          DELIVERY_ARTIFACT.DELIVERY_CONFIG_NAME,
          DELIVERY_ARTIFACT.IS_PREVIEW,
          ARTIFACT_LAST_CHECKED.AT
        )
          .from(DELIVERY_ARTIFACT, ARTIFACT_LAST_CHECKED)
          .where(DELIVERY_ARTIFACT.UID.eq(ARTIFACT_LAST_CHECKED.ARTIFACT_UID))
          .and(ARTIFACT_LAST_CHECKED.AT.lessOrEqual(cutoff))
          .orderBy(ARTIFACT_LAST_CHECKED.AT)
          .limit(limit)
          .forUpdate()
          .fetch()
          .onEach { (uid, _, _, _, _, deliveryConfigName, _, lastCheckedAt) ->
            insertInto(ARTIFACT_LAST_CHECKED)
              .set(ARTIFACT_LAST_CHECKED.ARTIFACT_UID, uid)
              .set(ARTIFACT_LAST_CHECKED.AT, now)
              .onDuplicateKeyUpdate()
              .set(ARTIFACT_LAST_CHECKED.AT, now)
              .execute()
            publisher.publishEvent(
              AboutToBeChecked(
                lastCheckedAt,
                "artifact",
                "deliveryConfig:$deliveryConfigName"
              )
            )
          }
          .map { (_, name, type, details, reference, deliveryConfigName, isPreview) ->
            objectMapper.mapToArtifact(artifactSuppliers.supporting(type), name, type, details, reference, deliveryConfigName, isPreview)
          }
      }
    }
  }

  override fun versionsApprovedBetween(
    deliveryConfig: DeliveryConfig,
    environmentName: String,
    startTime: Instant,
    endTime: Instant
  ): Int {
    require(startTime < endTime) {
      "Start time $startTime must be before end time $endTime"
    }
    log.debug(
      "checking for artifact versions approved for {}:{} between {} and {}",
      deliveryConfig.name,
      environmentName,
      startTime,
      endTime
    )
    return sqlRetry.withRetry(READ) {
      jooq
        .selectCount()
        .from(ENVIRONMENT_ARTIFACT_VERSIONS)
        .join(ENVIRONMENT).on(ENVIRONMENT.UID.eq(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID))
        .join(DELIVERY_CONFIG).on(DELIVERY_CONFIG.UID.eq(ENVIRONMENT.DELIVERY_CONFIG_UID))
        .where(DELIVERY_CONFIG.NAME.eq(deliveryConfig.name))
        .and(ENVIRONMENT.NAME.eq(environmentName))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.APPROVED_AT.between(startTime, endTime))
        .fetchSingleInto<Int>()
    }
  }

  override fun versionsDeployedBetween(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    environmentName: String,
    startTime: Instant,
    endTime: Instant
  ): Int =
    versionsInStatusBetween(deliveryConfig, artifact, environmentName, DEPLOYING, startTime, endTime)

  override fun versionsInStatusBetween(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    environmentName: String,
    status: PromotionStatus,
    startTime: Instant,
    endTime: Instant
  ): Int {
    require(startTime < endTime) {
      "Start time $startTime must be before end time $endTime"
    }
    log.debug(
      "checking for artifact versions for {}:{} with $status status between {} and {}",
      deliveryConfig.name,
      environmentName,
      startTime,
      endTime
    )
    return sqlRetry.withRetry(READ) {
      jooq
        .selectCount()
        .from(ARTIFACT_PROMOTION_EVENT)
        .where(ARTIFACT_PROMOTION_EVENT.ENVIRONMENT_UID.eq(deliveryConfig.getUidFor(environmentName)))
        .and(ARTIFACT_PROMOTION_EVENT.ARTIFACT_UID.eq(artifact.uid))
        .and(ARTIFACT_PROMOTION_EVENT.PROMOTION_STATUS.eq(status.name))
        .and(ARTIFACT_PROMOTION_EVENT.TIMESTAMP.between(startTime, endTime))
        .fetchSingleInto<Int>()
    }
  }

  override fun getApprovedInEnvArtifactVersion(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    environmentName: String,
    excludeCurrent: Boolean?
  ): PublishedArtifact? {
    val environment = deliveryConfig.environmentNamed(environmentName)
    val envUid = deliveryConfig.getUidFor(environment)
    val artifactId = artifact.uid

    val excludedStatuses = mutableListOf(VETOED)
     if (excludeCurrent == true) {
       excludedStatuses.add(CURRENT)
     }

    return latestApprovedArtifact(envUid, artifactId, excludedStatuses, artifact)
  }

  override fun getPreviouslyDeployedArtifactVersion(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    environmentName: String
  ): PublishedArtifact? {
    val currentVersion = getCurrentlyDeployedArtifactVersion(deliveryConfig, artifact, environmentName) ?: return null
    val environment = deliveryConfig.environmentNamed(environmentName)
    val envUid = deliveryConfig.getUidFor(environment)
    val artifactId = artifact.uid

    return sqlRetry.withRetry(READ) {
      jooq
        .selectArtifactVersionColumns()
        .from(ARTIFACT_VERSIONS, ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.eq(ARTIFACT_VERSIONS.VERSION))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(envUid))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifactId))
        .and(ARTIFACT_VERSIONS.NAME.eq(artifact.name)) // match name also, so if the artifact has been updated we match only the correct versions
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.REPLACED_BY.eq(currentVersion.version))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.eq(PREVIOUS))
        .fetchSortedArtifactVersions(artifact, 10).firstOrNull {
          // This ensures that we only pick older versions than the current one
          artifact.sortingStrategy.comparator.compare(
            it,
            currentVersion
          ) > 0
        }
    }
  }

  override fun versionsInUse(artifact: DeliveryArtifact): Set<String> =
    jooq
      .select(ENVIRONMENT_VERSION_ARTIFACT_VERSION.ARTIFACT_VERSION)
      .from(ENVIRONMENT_VERSION_ARTIFACT_VERSION)
      .where(ENVIRONMENT_VERSION_ARTIFACT_VERSION.ARTIFACT_UID.eq(artifact.uid))
      .andExists(
        selectOne()
          .from(ACTIVE_ENVIRONMENT)
          .where(ENVIRONMENT_VERSION_ARTIFACT_VERSION.ENVIRONMENT_UID.eq(ACTIVE_ENVIRONMENT.UID))
          .and(ENVIRONMENT_VERSION_ARTIFACT_VERSION.ENVIRONMENT_VERSION.eq(ACTIVE_ENVIRONMENT.VERSION))
      )
      .fetchSet(ENVIRONMENT_VERSION_ARTIFACT_VERSION.ARTIFACT_VERSION)

  private fun priorVersionDeployedIn(
    environmentId: String,
    artifactId: String,
    currentVersion: String
  ): String? {
    return sqlRetry.withRetry(READ) {
      jooq
        .select(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION)
        .from(ENVIRONMENT_ARTIFACT_VERSIONS)
        .where(ENVIRONMENT_ARTIFACT_VERSIONS.ENVIRONMENT_UID.eq(environmentId))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_UID.eq(artifactId))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION.ne(currentVersion))
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT.isNotNull)
        .and(ENVIRONMENT_ARTIFACT_VERSIONS.PROMOTION_STATUS.ne(VETOED))
        .orderBy(ENVIRONMENT_ARTIFACT_VERSIONS.DEPLOYED_AT.desc())
        .limit(1)
        .fetchOne(ENVIRONMENT_ARTIFACT_VERSIONS.ARTIFACT_VERSION)
    }
  }

  private fun deletePin(envUid: String, artUid: String) {
    // Deletes rows by primary key
    jooq.deleteFrom(ENVIRONMENT_ARTIFACT_PIN)
      .where(
        ENVIRONMENT_ARTIFACT_PIN.ENVIRONMENT_UID.eq(envUid),
        ENVIRONMENT_ARTIFACT_PIN.ARTIFACT_UID.eq(artUid)
      )
      .execute()
  }

  fun recordEnvArtifactVersionEvent(config: DeliveryConfig, environment: Environment, artifact: DeliveryArtifact, version: String, status: PromotionStatus, txn: DSLContext) {
    txn.insertInto(ARTIFACT_PROMOTION_EVENT)
      .set(ARTIFACT_PROMOTION_EVENT.UID, ULID().nextULID(clock.millis())) // so that uids are cronologically sortable
      .set(ARTIFACT_PROMOTION_EVENT.ENVIRONMENT_UID, config.getUidFor(environment))
      .set(ARTIFACT_PROMOTION_EVENT.ARTIFACT_UID, artifact.uid)
      .set(ARTIFACT_PROMOTION_EVENT.ARTIFACT_VERSION, version)
      .set(ARTIFACT_PROMOTION_EVENT.PROMOTION_STATUS, status.name)
      .set(ARTIFACT_PROMOTION_EVENT.TIMESTAMP, clock.instant())
      .execute()

  }
  fun batchRecordEnvArtifactVersionEvent(config: DeliveryConfig, environment: Environment, artifact: DeliveryArtifact, versions: List<String>, status: PromotionStatus, txn: DSLContext) {
    val envUid = config.getUidStringFor(environment)
    val artifactId = artifact.uidString
     txn
      .insertInto(ARTIFACT_PROMOTION_EVENT,
        ARTIFACT_PROMOTION_EVENT.UID,
        ARTIFACT_PROMOTION_EVENT.ENVIRONMENT_UID,
        ARTIFACT_PROMOTION_EVENT.ARTIFACT_UID,
        ARTIFACT_PROMOTION_EVENT.ARTIFACT_VERSION,
        ARTIFACT_PROMOTION_EVENT.PROMOTION_STATUS,
        ARTIFACT_PROMOTION_EVENT.TIMESTAMP,
      )
       .apply {
          versions.forEach {
            values(ULID().nextULID(clock.millis()), envUid, artifactId, it, status.name, clock.instant())
          }
      }
      .execute()
    }

  @EventListener(ApplicationUp::class)
  fun cleanEnvArtifactVersionEvents() {
    sqlRetry.withRetry(WRITE) {
      val numDeleted = jooq
        .deleteFrom(ARTIFACT_PROMOTION_EVENT)
        .where(ARTIFACT_PROMOTION_EVENT.TIMESTAMP.lessOrEqual(clock.instant().minus(Duration.ofDays(30))))
        .execute()
      log.debug("Deleted $numDeleted records in ARTIFACT_PROMOTION_EVENT table")
    }
  }

  private fun DeliveryConfig.getUidFor(environment: Environment): Select<Record1<String>> =
    select(ACTIVE_ENVIRONMENT.UID)
      .from(ACTIVE_ENVIRONMENT)
      .where(ACTIVE_ENVIRONMENT.NAME.eq(environment.name))
      .and(ACTIVE_ENVIRONMENT.DELIVERY_CONFIG_UID.eq(uid))

  private fun DeliveryConfig.getUidFor(environmentName: String): Select<Record1<String>> =
    select(ACTIVE_ENVIRONMENT.UID)
      .from(ACTIVE_ENVIRONMENT)
      .where(ACTIVE_ENVIRONMENT.NAME.eq(environmentName))
      .and(ACTIVE_ENVIRONMENT.DELIVERY_CONFIG_UID.eq(uid))

  private fun DeliveryConfig.getUidStringFor(environment: Environment): String =
    jooq.select(ACTIVE_ENVIRONMENT.UID)
      .from(ACTIVE_ENVIRONMENT)
      .where(ACTIVE_ENVIRONMENT.NAME.eq(environment.name))
      .and(ACTIVE_ENVIRONMENT.DELIVERY_CONFIG_UID.eq(uid))
      .fetchOne(ACTIVE_ENVIRONMENT.UID) ?: error("environment not found for $name / ${environment.name}")

  private val DeliveryArtifact.uid: Select<Record1<String>>
    get() = select(DELIVERY_ARTIFACT.UID)
      .from(DELIVERY_ARTIFACT)
      .where(
        DELIVERY_ARTIFACT.NAME.eq(name)
          .and(DELIVERY_ARTIFACT.TYPE.eq(type))
          .and(DELIVERY_ARTIFACT.DELIVERY_CONFIG_NAME.eq(deliveryConfigName))
          .and(DELIVERY_ARTIFACT.REFERENCE.eq(reference))
      )

  private val DeliveryArtifact.uidString: String
    get() = sqlRetry.withRetry(READ) {
      jooq.select(DELIVERY_ARTIFACT.UID)
        .from(DELIVERY_ARTIFACT)
        .where(
          DELIVERY_ARTIFACT.NAME.eq(name)
            .and(DELIVERY_ARTIFACT.TYPE.eq(type))
            .and(DELIVERY_ARTIFACT.DELIVERY_CONFIG_NAME.eq(deliveryConfigName))
            .and(DELIVERY_ARTIFACT.REFERENCE.eq(reference))
        )
        .fetchOne(DELIVERY_ARTIFACT.UID) ?: error(
        "artifact not found for " +
          "name=$name, " +
          "type=$type, " +
          "deliveryConfig=$deliveryConfigName, " +
          "reference=$reference"
      )
    }

  private val DeliveryConfig.uid: Select<Record1<String>>
    get() = select(DELIVERY_CONFIG.UID)
      .from(DELIVERY_CONFIG)
      // TODO: currently this is unique but I feel like it should be a compound key with application name
      .where(DELIVERY_CONFIG.NAME.eq(name))

  // Generates a unique hash for an artifact
  private fun DeliveryArtifact.fingerprint(): String {
    return fingerprint(name, type, deliveryConfigName ?: "_pending", reference)
  }

  private fun fingerprint(name: String, type: String, deliveryConfigName: String, reference: String): String {
    val data = name + type + deliveryConfigName + reference
    val bytes = MessageDigest
      .getInstance("SHA-1")
      .digest(data.toByteArray())
    return DatatypeConverter.printHexBinary(bytes).uppercase()
  }

  private fun vetoKey(envName: String, artifactId: String) = "$envName:$artifactId"

  private val log by lazy { LoggerFactory.getLogger(javaClass) }
}
