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
  fun get(): Type =
    if (environment.activeProfiles.contains("laptop")) Type.LAPTOP else Type.CLOUD

  enum class Type {
    LAPTOP,
    CLOUD
  }
}
