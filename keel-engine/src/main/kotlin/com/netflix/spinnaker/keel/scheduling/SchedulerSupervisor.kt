package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.scheduling.activities.SupervisorActivities
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

/**
 * Supervises all resource schedulers, ensuring that only managed resources have scheduler workflows running.
 */
@WorkflowInterface
interface SchedulerSupervisor {

  @WorkflowMethod
  fun supervise(request: SuperviseRequest)

  data class SuperviseRequest(
    val scheduler: SupervisorType
  )

  enum class SupervisorType(val type: String) {
    RESOURCE("resource"),
    ENVIRONMENT("environment")
  }
}

class DefaultSchedulerSupervisor : SchedulerSupervisor {

  private val supervisorActivities = SupervisorActivities.get()

  override fun supervise(request: SchedulerSupervisor.SuperviseRequest) {
    supervisorActivities.reconcileSchedulers(SupervisorActivities.ReconcileSchedulersRequest(request.scheduler))
  }
}
