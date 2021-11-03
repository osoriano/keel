package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import com.netflix.spinnaker.time.MutableClock
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo
import strikt.assertions.isNotNull
import java.net.InetAddress

class SqlHeartTests {
  private val jooq = testDatabase.context
  private val clock = MutableClock()
  private val sqlRetry = RetryProperties(1, 0).let { SqlRetry(SqlRetryProperties(it, it)) }
  private val heart = SqlHeart(jooq, sqlRetry, clock)
  
  @AfterEach
  fun flush() {
    SqlTestUtil.cleanupDb(jooq)
  }

  @Test
  fun `insert new heartbeat`() {
    heart.beat()
    expectThat(heart.getLastBeat(InetAddress.getLocalHost().hostName)).isNotNull().isEqualTo(clock.instant())
  }

  @Test
  fun `update heartbeat`() {
    val time1 = clock.instant()
    heart.beat()
    clock.tickMinutes(5)
    val time2 = clock.instant()
    heart.beat()
    expectThat(time1).isNotEqualTo(time2)
    expectThat(heart.getLastBeat(InetAddress.getLocalHost().hostName)).isNotNull().isEqualTo(time2)
  }

  @Test
  fun `won't clean fresh beats`() {
    heart.beat()
    clock.tickMinutes(15)
    expectThat(heart.cleanOldRecords()).isEqualTo(0)
  }

  @Test
  fun `will clean old beats`() {
    heart.beat()
    clock.tickMinutes(70)
    expectThat(heart.cleanOldRecords()).isEqualTo(1)
  }
}
