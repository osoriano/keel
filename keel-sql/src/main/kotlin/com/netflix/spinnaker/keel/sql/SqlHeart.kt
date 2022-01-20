package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.keel.persistence.Heart
import com.netflix.spinnaker.keel.persistence.metamodel.tables.Heartbeat.HEARTBEAT
import com.netflix.spinnaker.keel.sql.RetryCategory.READ
import com.netflix.spinnaker.keel.sql.RetryCategory.WRITE
import org.jooq.DSLContext
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.net.InetAddress
import java.time.Clock
import java.time.Duration
import java.time.Instant

class SqlHeart(
  val jooq: DSLContext,
  val sqlRetry: SqlRetry,
  val clock: Clock,
): Heart {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  @Scheduled(fixedDelayString = "\${keel.heartbeat.frequency.ms:5000}") // heartbeat every 5 seconds
  override fun beat() {
    sqlRetry.withRetry(READ) {
      jooq.insertInto(HEARTBEAT)
        .set(HEARTBEAT.IDENTITY, InetAddress.getLocalHost().hostName)
        .set(HEARTBEAT.LAST_HEARTBEAT, clock.instant())
        .onDuplicateKeyUpdate()
        .set(HEARTBEAT.LAST_HEARTBEAT, clock.instant())
        .execute()
    }
  }

  @Scheduled(fixedDelayString = "PT1H")
  fun cleanOldRecords(): Int =
    sqlRetry.withRetry(WRITE) {
      val numDeleted = jooq.deleteFrom(HEARTBEAT)
        .where(HEARTBEAT.LAST_HEARTBEAT.le(clock.instant().minus(Duration.ofHours(1))))
        .execute()

      log.info("Instance ${InetAddress.getLocalHost().hostName} deleted $numDeleted records from the heartbeat table")
      numDeleted
    }

  fun getLastBeat(identity: String): Instant? =
    sqlRetry.withRetry(WRITE) {
      jooq.select(HEARTBEAT.LAST_HEARTBEAT)
        .from(HEARTBEAT)
        .where(HEARTBEAT.IDENTITY.eq(identity))
        .fetchOne(HEARTBEAT.LAST_HEARTBEAT)
    }
}
