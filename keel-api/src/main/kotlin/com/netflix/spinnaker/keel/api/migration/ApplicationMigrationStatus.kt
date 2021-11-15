package com.netflix.spinnaker.keel.api.migration


data class ApplicationMigrationStatus(
  val isMigratable: Boolean, // this value defines if we can initiate the migration process
  val deliveryConfig: Map<String, Any?>? = null
)
