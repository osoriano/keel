package com.netflix.spinnaker.keel.api

/**
 * An optional component of the submitted environment that informs which workload type should be used
 * when determining locations for resources that have not explicitly specified them.
 *
 * ```
 * environments:
 *   name: test
 *     workloadType:
 *       id: test/spinnaker     ## of the form <platform environment>/<workload type id>
 *       overrides:
 *         accountType: main    ## optional (uncommon)
 *         cloudProvider: aws   ## optional (uncommon)
 * ```
 */
data class WorkloadType(val id: String, val overrides: WorkloadTypeOverrides = WorkloadTypeOverrides()) {
  init {
    require(id.matches(Regex("^.*/.*$"))) {
      "WorkloadType id must be of the form <platformEnvironment>/<workloadType>"
    }
  }

  fun platformEnvironment() = id.split("/")[0]
  fun workloadTypeId() = id.split("/")[1]

  data class WorkloadTypeOverrides(
    val accountType: String? = null,
    val cloudProvider: String? = null
  )
}
