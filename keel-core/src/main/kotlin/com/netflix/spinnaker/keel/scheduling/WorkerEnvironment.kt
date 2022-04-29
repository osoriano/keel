package com.netflix.spinnaker.keel.scheduling

import org.springframework.core.env.Environment
import org.springframework.stereotype.Component

/**
 * We don't want a laptop Keel process to mess with workflows that have been created in a cloud environment.
 * Specifically, the `SchedulerSupervisor` in keel-engine, will attempt to deschedule resources that are unknown to the
 * database. This class provides an environment name that can be attached to workflows to have better provenance
 * information.
 */
@Component
class WorkerEnvironment(
  private val environment: Environment
) {
  fun get(): String =
    if (environment.activeProfiles.contains("laptop")) getLaptopName() else CLOUD

  /**
   * Produces environment names for laptop, e.g. "laptop:rzienert"
   */
  private fun getLaptopName(): String =
    System.getProperty("user.name")
      .ifEmpty {
        throw IllegalStateException("Could not resolve laptop user (user.name property should be set)")
      }
      .let { "laptop:$it" }

  private companion object {
    // Capitalized for backwards compatibility with prior enum formatting. Changes would be unwelcome (may cause
    // unnecessary termination & rescheduling of workflows)
    private const val CLOUD = "CLOUD"
  }
}
