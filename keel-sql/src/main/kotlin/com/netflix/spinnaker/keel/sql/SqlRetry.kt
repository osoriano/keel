package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.config.ConnectionPools
import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.config.FeatureToggles.Companion.USE_READ_REPLICA
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.routing.withPool
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import io.vavr.control.Try
import java.time.Duration
import org.jooq.exception.SQLDialectNotSupportedException

class SqlRetry(
  private val sqlRetryProperties: SqlRetryProperties,
  private val featureToggles: FeatureToggles
) {
  fun <T> withRetry(category: RetryCategory, action: () -> T): T {
    return if (category == RetryCategory.WRITE) {
      val retry = Retry.of(
        "sqlWrite",
        RetryConfig.custom<T>()
          .maxAttempts(sqlRetryProperties.transactions.maxRetries)
          .waitDuration(Duration.ofMillis(sqlRetryProperties.transactions.backoffMs))
          .ignoreExceptions(SQLDialectNotSupportedException::class.java)
          .build()
      )
      withPool(ConnectionPools.DEFAULT.value) {
        Try.ofSupplier(Retry.decorateSupplier(retry, action)).get()
      }
    } else {
      val retry = Retry.of(
        "sqlRead",
        RetryConfig.custom<T>()
          .maxAttempts(sqlRetryProperties.reads.maxRetries)
          .waitDuration(Duration.ofMillis(sqlRetryProperties.reads.backoffMs))
          .ignoreExceptions(SQLDialectNotSupportedException::class.java)
          .build()
      )

      val connectionPool = if (featureToggles.isEnabled(USE_READ_REPLICA, false)) {
        ConnectionPools.READ_ONLY.value
      } else {
        ConnectionPools.DEFAULT.value
      }
      
      withPool(connectionPool) {
        Try.ofSupplier(Retry.decorateSupplier(retry, action)).get()
      }
    }
  }
}

enum class RetryCategory {
  WRITE, READ
}
