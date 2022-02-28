package com.netflix.spinnaker.keel.persistence

import com.netflix.spinnaker.keel.application.ApplicationConfig
import java.time.Instant

interface ApplicationRepository {
  fun get(application: String): ApplicationConfig?
  fun store(config: ApplicationConfig)
  fun isAutoImportEnabled(application: String): Boolean
}
