package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.keel.application.ApplicationConfig
import com.netflix.spinnaker.keel.persistence.ApplicationRepository
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.APPLICATION_CONFIG
import org.jooq.DSLContext
import java.time.Clock

class SqlApplicationRepository(
  private val jooq: DSLContext,
  private val clock: Clock,
  private val sqlRetry: SqlRetry
) : ApplicationRepository {

  override fun get(application: String): ApplicationConfig? {
    return sqlRetry.withRetry(RetryCategory.READ) {
      jooq.select(
        APPLICATION_CONFIG.APPLICATION,
        APPLICATION_CONFIG.AUTO_IMPORT,
        APPLICATION_CONFIG.DELIVERY_CONFIG_PATH,
        APPLICATION_CONFIG.UPDATED_AT,
        APPLICATION_CONFIG.UPDATED_BY
      ).from(APPLICATION_CONFIG)
        .where(APPLICATION_CONFIG.APPLICATION.eq(application))
        .fetchOne { (application, autoImport, configPath, updatedAt, updatedBy) ->
        ApplicationConfig(
          application = application,
          autoImport = autoImport,
          deliveryConfigPath = configPath,
          updatedAt = updatedAt,
          updatedBy = updatedBy
        )
      }
    }
  }

  override fun store(config: ApplicationConfig) {
    val now = clock.instant()
    sqlRetry.withRetry(RetryCategory.WRITE) {
      jooq.insertInto(APPLICATION_CONFIG)
        .set(APPLICATION_CONFIG.APPLICATION, config.application)
        .set(APPLICATION_CONFIG.AUTO_IMPORT, config.autoImport)
        .set(APPLICATION_CONFIG.DELIVERY_CONFIG_PATH, config.deliveryConfigPath)
        .set(APPLICATION_CONFIG.UPDATED_AT, now)
        .set(APPLICATION_CONFIG.UPDATED_BY, config.updatedBy)
        .onDuplicateKeyUpdate()
        .set(APPLICATION_CONFIG.AUTO_IMPORT, config.autoImport)
        .set(APPLICATION_CONFIG.DELIVERY_CONFIG_PATH, config.deliveryConfigPath)
        .set(APPLICATION_CONFIG.UPDATED_AT, now)
        .set(APPLICATION_CONFIG.UPDATED_BY, config.updatedBy)
        .execute()
    }
  }

}
