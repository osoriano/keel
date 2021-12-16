package com.netflix.spinnaker.keel.front50.model

import java.time.Instant

data class ServiceAccount(
  val name: String,
  val lastModifiedBy: String?,
  val lastModified: Instant?,
  val memberOf: List<String> = emptyList()
)
