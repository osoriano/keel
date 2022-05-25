package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.keel.scheduling.TemporalAdminService
import com.netflix.spinnaker.keel.scheduling.TemporalSupervisorAdminService
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * Exposes temporal admin endpoints
 */
@RestController
@RequestMapping(path = ["/poweruser/temporal"])
class TemporalAdminController(
  private val temporalAdminService: TemporalAdminService,
  private val temporalSupervisorAdminService: TemporalSupervisorAdminService
) {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  @PostMapping(
    path = ["/reset/resourceSupervisor"]
  )
  fun resetResourceSupervisor() {
    temporalSupervisorAdminService.resetResourceSupervisor()
  }

  @PostMapping(
    path = ["/reset/environmentSupervisor"]
  )
  fun resetEnvironmentSupervisor() {
    temporalSupervisorAdminService.resetEnvironmentSupervisor()
  }

  @PostMapping(
    path = ["/recheck/resources"]
  )
  fun recheckResources() {
    temporalAdminService.checkAllResourcesNow()
  }

  @PostMapping(
    path = ["/recheck/environments"]
  )
  fun recheckEnvironments() {
    temporalAdminService.checkAllEnvironmentsNow()
  }

  @PostMapping(
    path = ["/reset/resources"]
  )
  fun resetResources() {
    temporalAdminService.resetAllResourceWorkflowsAsync()
  }

  @PostMapping(
    path = ["/reset/environments"]
  )
  fun resetEnvironments() {
    temporalAdminService.resetAllEnvironmentWorkflowsAsync()
  }

  @PostMapping(
    path = ["/terminate/resources"]
  )
  fun terminateResources() {
    temporalAdminService.terminateAllResourceWorkflowsAsync()
  }

  @PostMapping(
    path = ["/terminate/environments"]
  )
  fun terminateEnvironments() {
    temporalAdminService.terminateAllEnvironmentWorkflowsAsync()
  }
}
