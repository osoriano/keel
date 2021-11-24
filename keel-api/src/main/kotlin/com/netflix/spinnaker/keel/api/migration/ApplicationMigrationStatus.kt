package com.netflix.spinnaker.keel.api.migration


data class ApplicationMigrationStatus(
  val isMigratable: Boolean = false, // this value defines if we can initiate the migration process
  val isBlocked: Boolean = false,
  val alreadyManaged: Boolean = false,
  val isScmPowered: Boolean = false,
  val deliveryConfig: Map<String, Any?>? = null
)
