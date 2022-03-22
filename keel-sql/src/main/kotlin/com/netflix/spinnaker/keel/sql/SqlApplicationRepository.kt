package com.netflix.spinnaker.keel.sql

import com.netflix.kotlin.OpenClass
import com.netflix.spinnaker.keel.application.ApplicationConfig
import com.netflix.spinnaker.keel.persistence.ApplicationRepository
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.APPLICATION_CONFIG
import org.jooq.DSLContext
import java.time.Clock
import java.time.Instant

@OpenClass
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
        .apply {
          if(config.autoImport != null) {
            set(APPLICATION_CONFIG.AUTO_IMPORT, config.autoImport)
          }
          if (config.deliveryConfigPath != null) {
            set(APPLICATION_CONFIG.DELIVERY_CONFIG_PATH, config.deliveryConfigPath)
          }
        }
        .set(APPLICATION_CONFIG.UPDATED_AT, now)
        .set(APPLICATION_CONFIG.UPDATED_BY, config.updatedBy)
        .onDuplicateKeyUpdate()
        .apply {
          if(config.autoImport != null) {
            set(APPLICATION_CONFIG.AUTO_IMPORT, config.autoImport)
          }
          if (config.deliveryConfigPath != null) {
            set(APPLICATION_CONFIG.DELIVERY_CONFIG_PATH, config.deliveryConfigPath)
          }
        }
        .set(APPLICATION_CONFIG.UPDATED_AT, now)
        .set(APPLICATION_CONFIG.UPDATED_BY, config.updatedBy)
        .execute()
    }
  }

  override fun isAutoImportEnabled(application: String): Boolean {
    return sqlRetry.withRetry(RetryCategory.READ) {
      jooq.select(APPLICATION_CONFIG.AUTO_IMPORT).from(APPLICATION_CONFIG)
        .where(APPLICATION_CONFIG.APPLICATION.eq(application))
        .fetchOne(APPLICATION_CONFIG.AUTO_IMPORT)
    } == true
  }
}
