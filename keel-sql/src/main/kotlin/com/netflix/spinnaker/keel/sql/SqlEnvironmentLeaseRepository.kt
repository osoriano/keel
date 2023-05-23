package com.netflix.spinnaker.keel.sql

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.histogram.PercentileTimer
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.core.api.UID
import com.netflix.spinnaker.keel.core.api.randomUID
import com.netflix.spinnaker.keel.exceptions.ActiveLeaseExists
import com.netflix.spinnaker.keel.persistence.EnvironmentLeaseRepository
import com.netflix.spinnaker.keel.persistence.Lease
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ACTIVE_ENVIRONMENT
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.DELIVERY_CONFIG
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ENVIRONMENT_LEASE
import com.netflix.spinnaker.keel.persistence.metamodel.tables.records.EnvironmentLeaseRecord
import com.netflix.spinnaker.keel.telemetry.recordDurationPercentile
import org.jooq.DSLContext
import org.jooq.exception.DataAccessException
import org.slf4j.LoggerFactory
import org.springframework.dao.DataIntegrityViolationException
import java.net.InetAddress
import java.sql.SQLIntegrityConstraintViolationException
import java.time.Clock
import java.time.Duration
import java.time.Instant

/**
 * An implementation of [EnvironmentLeaseRepository] that represents a lease as a record in the environment_lease
 * table.
 */
class SqlEnvironmentLeaseRepository(
  private val jooq: DSLContext,
  private val clock: Clock,
  private val spectator: Registry,
  private val leaseDuration: Duration) : EnvironmentLeaseRepository {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  private val leaseCountId = spectator.createId("lease.env.count")


  /**
   * Check the database to see if there is an active lease for [environment]
   *
   * @return a valid Lease object on success
   *
   * @throws ActiveLeaseExists if another client is holding a lease
   *
   */
  override fun tryAcquireLease(deliveryConfig: DeliveryConfig, environment: Environment, actionType: String): Lease {
    val startTime = clock.instant()

    try {
      val environmentUid = getEnvironmentUid(deliveryConfig, environment)
      val leaseUid = randomUID()

      jooq.inTransaction {

        val record: EnvironmentLeaseRecord? = selectFrom(ENVIRONMENT_LEASE)
          .where(ENVIRONMENT_LEASE.ENVIRONMENT_UID.eq(environmentUid))
          .forUpdate()
          .fetchOne()

        when {
          // no existing lease
          record == null -> insertRecord(this, leaseUid, environmentUid, actionType, deliveryConfig.application, environment.name)
            .also { leaseCountId.incrementGranted("free", actionType, deliveryConfig.application, environment.name) }

          // expired lease
          isExpired(record.leasedAt) -> updateRecord(this, leaseUid, environmentUid, actionType)
            .also {
              log.warn("Got expired lease for ${deliveryConfig.application}/${environment.name}")
              leaseCountId.incrementGranted("expired", actionType, deliveryConfig.application, environment.name)
            }

          // active lease
          else -> throw ActiveLeaseExists(environment.name, record.leasedBy, record.leasedAt)
            .also {
              log.error("Lease already exists for ${deliveryConfig.application}/${environment.name}")
              leaseCountId.incrementDenied(actionType, deliveryConfig.application, environment.name)
            }
        }
      }
      return SqlLease(this, environmentUid, leaseUid, startTime, actionType, spectator, clock)

    } catch (e: DataAccessException) {
      recordDeniedLeaseTime(startTime, actionType)

      // jooq.inTransaction wraps our exception in a DataAccessException so we need to unwrap it
      (e.cause as? ActiveLeaseExists)
        ?.let { throw it }
        ?: throw e
    } catch (e: Exception) {
      recordDeniedLeaseTime(startTime, actionType)
      throw e
    }
  }

  override fun tryAcquireLease(environmentUid: String, actionType: String): Lease {
    val startTime = clock.instant()

    try {
      val leaseUid = randomUID()

      jooq.inTransaction {

        val record: EnvironmentLeaseRecord? = selectFrom(ENVIRONMENT_LEASE)
          .where(ENVIRONMENT_LEASE.ENVIRONMENT_UID.eq(environmentUid))
          .forUpdate()
          .fetchOne()

        when {
          // no existing lease
          record == null -> insertRecord(this, leaseUid, environmentUid, actionType, "keelapplication", "keelenvironment")
            .also { leaseCountId.incrementGranted("free", actionType, "keelapplication", "keelenvironment") }

          // expired lease
          isExpired(record.leasedAt) -> updateRecord(this, leaseUid, environmentUid, actionType)
            .also {
              log.warn("Got expired lease for ${environmentUid}")
              leaseCountId.incrementGranted("expired", actionType, "keelapplication", "keelenvironment")
            }

          // active lease
          else -> throw ActiveLeaseExists(environmentUid, record.leasedBy, record.leasedAt)
            .also {
              log.error("Lease already exists for ${environmentUid}")
              leaseCountId.incrementDenied(actionType, "keelapplication", "keelenvironment")
            }
        }
      }
      return SqlLease(this, environmentUid, leaseUid, startTime, actionType, spectator, clock)

    } catch (e: DataAccessException) {
      recordDeniedLeaseTime(startTime, actionType)

      // jooq.inTransaction wraps our exception in a DataAccessException so we need to unwrap it
      (e.cause as? ActiveLeaseExists)
        ?.let { throw it }
        ?: throw e
    } catch (e: Exception) {
      recordDeniedLeaseTime(startTime, actionType)
      throw e
    }
  }

  /**
   * Percentile timer for measuring how long the lease is held
   *
   * We use the default percentile time range: 10ms to 1 minute
   */
  private fun recordDeniedLeaseTime(startTime : Instant, actionType: String) =
    spectator.recordDurationPercentile(
      "lease.env.duration",
      clock,
      startTime,
      setOf(BasicTag("action", actionType), BasicTag("outcome", "denied")))

  /**
   * Return true if the expiration date of the lease is in the past
   */
  private fun isExpired(leasedAt: Instant): Boolean {
    val expirationDate = leasedAt + leaseDuration
    val now = clock.instant()
    return expirationDate < now
  }

  private fun insertRecord(
    ctx: DSLContext,
    uid: UID,
    environmentUid: String,
    actionType: String,
    application: String,
    environmentName: String
  ) {
    try {
      ctx.insertInto(ENVIRONMENT_LEASE)
        .set(ENVIRONMENT_LEASE.UID, uid.toString())
        .set(ENVIRONMENT_LEASE.ENVIRONMENT_UID, environmentUid)
        .set(ENVIRONMENT_LEASE.LEASED_BY, lesseeIdentifier())
        .set(ENVIRONMENT_LEASE.LEASED_AT, clock.instant())
        .set(ENVIRONMENT_LEASE.COMMENT, actionType)
        .execute()
    } catch (e: DataAccessException) {
      // jooq.inTransaction wraps our exception in a DataAccessException so we need to unwrap it
      (e.cause as? SQLIntegrityConstraintViolationException)
        ?.let {
            log.error("Lease already exists for $application/${environmentName}")
            leaseCountId.incrementDenied(actionType, application, environmentName)
          throw ActiveLeaseExists(environmentName, "insertConflict", clock.instant())
        } ?: throw e
    } catch (e: DataIntegrityViolationException) {
      (e.cause as? SQLIntegrityConstraintViolationException)
        ?.let {
            log.error("Lease already exists for $application/${environmentName}")
            leaseCountId.incrementDenied(actionType, application, environmentName)
          throw ActiveLeaseExists(environmentName, "insertConflict", clock.instant())
        } ?: throw e
    }
  }

  /**
   * Functionally, this method deletes the expired lease and creates a new one.
   *
   * Instead of deleting the old record and inserting a new one, we simply update the uid
   */
  private fun updateRecord(ctx: DSLContext, uid: UID, environmentUid: String, comment: String) {
    ctx.update(ENVIRONMENT_LEASE)
      .set(ENVIRONMENT_LEASE.UID, uid.toString())
      .set(ENVIRONMENT_LEASE.LEASED_BY, lesseeIdentifier())
      .set(ENVIRONMENT_LEASE.LEASED_AT, clock.instant())
      .set(ENVIRONMENT_LEASE.COMMENT, comment)
      .where(ENVIRONMENT_LEASE.ENVIRONMENT_UID.eq(environmentUid))
      .execute()
  }

  fun release(lease: SqlLease) {
    jooq.inTransaction {

      val record: EnvironmentLeaseRecord? = selectFrom(ENVIRONMENT_LEASE)
        .where(ENVIRONMENT_LEASE.ENVIRONMENT_UID.eq(lease.environmentUid))
        .forUpdate()
        .fetchOne()

      when {
        // no existing lease
        record == null -> {
          log.warn("Lease for environment ${lease.environmentUid} missing. Operation may have taken longer "
            + "than the lease duration")
        }

        // end the current active lease
        record.uid == lease.leaseUid.toString() -> {
          deleteFrom(ENVIRONMENT_LEASE)
            .where(ENVIRONMENT_LEASE.ENVIRONMENT_UID.eq(lease.environmentUid))
            .execute()
        }

        // lease was granted to another transaction
        else -> {
          log.warn("Lease for environment ${lease.environmentUid} taken by another transaction. "
            + "Operation may have taken longer than the lease duration")
        }
      }
    }
  }

  private fun getEnvironmentUid(deliveryConfig: DeliveryConfig, environment: Environment): String =
    jooq.select(ACTIVE_ENVIRONMENT.UID)
      .from(DELIVERY_CONFIG)
      .join(ACTIVE_ENVIRONMENT)
      .on(ACTIVE_ENVIRONMENT.DELIVERY_CONFIG_UID.eq(DELIVERY_CONFIG.UID))
      .where(DELIVERY_CONFIG.NAME.eq(deliveryConfig.name))
      .and(ACTIVE_ENVIRONMENT.NAME.eq(environment.name))
      .fetchSingleInto<String>()

  /**
   * A string that identifies the client who took the lease
   */
  private fun lesseeIdentifier() : String =
    InetAddress.getLocalHost().hostName


  //
  // Metric helpers
  //

  private fun Id.incrementGranted(status: String, actionType: String, application: String, environment: String)  =
    increment("granted", status, actionType, application, environment)

  private fun Id.incrementDenied(actionType: String, application: String, environment: String) =
    increment("denied", "active", actionType, application, environment)

  private fun Id.increment(outcome: String, status: String, actionType: String, application: String, environment: String) {
    val id =
      this.withTags(
        "outcome", outcome,
        "status", status,
        "action", actionType,
        "application", application,
        "environment", environment)
    spectator.counter(id).increment()
  }

  class SqlLease(
    val repository: SqlEnvironmentLeaseRepository,
    val environmentUid: String,
    val leaseUid: UID,
    private val startTime: Instant,
    private val actionType: String,
    private val spectator: Registry,
    private val clock: Clock
  ) : Lease {
    override fun close() {
      repository.release(this)
      spectator.recordDurationPercentile(
        "lease.env.duration",
        clock,
        startTime,
        setOf(BasicTag("action", actionType), BasicTag("outcome", "granted")))
    }
  }
}
