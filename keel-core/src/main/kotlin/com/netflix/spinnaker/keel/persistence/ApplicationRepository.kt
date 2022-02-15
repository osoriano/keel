package com.netflix.spinnaker.keel.persistence

import com.netflix.spinnaker.keel.application.ApplicationConfig

interface ApplicationRepository {
  fun get(application: String): ApplicationConfig?
  fun store(config: ApplicationConfig)
}
