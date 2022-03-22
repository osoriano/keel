package com.netflix.spinnaker.keel.application

import java.time.Instant

data class ApplicationConfig(
  val application: String,
  val autoImport: Boolean?,
  val deliveryConfigPath: String? = DEFAULT_MANIFEST_PATH,
  val updatedAt: Instant? = null,
  val updatedBy: String? = null,
) {
  companion object {
    const val DEFAULT_MANIFEST_PATH = "spinnaker.yml"
  }
}
