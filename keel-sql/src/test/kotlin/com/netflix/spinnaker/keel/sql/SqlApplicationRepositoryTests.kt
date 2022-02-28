package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.keel.application.ApplicationConfig
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import com.netflix.spinnaker.time.MutableClock
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isNotEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isTrue

class SqlApplicationRepositoryTests {
  private val jooq = testDatabase.context
  private val clock = MutableClock()
  private val sqlRetry = RetryProperties(1, 0).let { SqlRetry(SqlRetryProperties(it, it)) }
  private val repository = SqlApplicationRepository(jooq, clock, sqlRetry)

  private val app = "fnord"
  private val config = ApplicationConfig(
    application = app,
    autoImport = true,
    deliveryConfigPath = "./spinnaker.yml",
    updatedAt = null,
    updatedBy = "someone"
  )

  private val config2 = ApplicationConfig(
    application = app,
    autoImport = false,
    deliveryConfigPath = "./spinnaker2.yml",
    updatedAt = null,
    updatedBy = "someone2"
  )

  @AfterEach
  fun flush() {
    SqlTestUtil.cleanupDb(jooq)
  }

  @Test
  fun `insert new app`() {
    repository.store(config)
    expectThat(repository.get(app)).isNotNull().get { updatedAt }.isNotNull()
  }

  @Test
  fun `update app`() {
    val time1 = clock.instant()
    repository.store(config)
    clock.tickMinutes(5)
    val time2 = clock.instant()
    repository.store(config2)
    expectThat(time1).isNotEqualTo(time2)
    expectThat(repository.get(app)).isNotNull().isEqualTo(config2.copy(updatedAt = time2))
  }

  @Test
  fun `adding another app`() {
    repository.store(config)
    repository.store(config.copy(application = "fnord2", autoImport = false))
    expectThat(repository.get(config.application)).isNotNull().get { autoImport }.isTrue()
    expectThat(repository.get("fnord2")).isNotNull().get { autoImport }.isFalse()
  }

  @Test
  fun `auto import`() {
    repository.store(config)
    expectThat(repository.isAutoImportEnabled(config.application)).isTrue()
  }
}
