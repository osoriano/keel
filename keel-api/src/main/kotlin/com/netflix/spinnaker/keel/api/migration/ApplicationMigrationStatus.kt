package com.netflix.spinnaker.keel.api.migration


data class ApplicationMigrationStatus(
  val exportSucceeded: Boolean = false,
  val inAllowList: Boolean = false,
  val assistanceNeeded: Boolean = false,
  val alreadyManaged: Boolean = false,
  val deliveryConfig: Map<String, Any?>? = null,
  val userGeneratedConfig: Map<String, Any?>? = null,
  val prLink: String? = null,
  val jiraLink: String? = null,
  val pipelines: List<MigrationPipeline>? = null
) {
  val isMigratable: Boolean
    get() = exportSucceeded && inAllowList

  val isBlocked: Boolean
    get() = assistanceNeeded
}
