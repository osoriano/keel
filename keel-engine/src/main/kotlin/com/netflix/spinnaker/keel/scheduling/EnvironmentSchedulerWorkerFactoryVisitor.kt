package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.scheduling.SchedulerSupervisor.SupervisorType
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.ENVIRONMENT_SCHEDULER_TASK_QUEUE
import com.netflix.spinnaker.keel.scheduling.activities.ActuatorActivities
import com.netflix.spinnaker.keel.scheduling.activities.SchedulingConfigActivities
import com.netflix.spinnaker.keel.scheduling.activities.SupervisorActivities
import com.netflix.temporal.core.convention.TaskQueueNamer
import io.temporal.client.WorkflowClient
import io.temporal.worker.WorkerFactory
import io.temporal.worker.WorkerOptions
import org.springframework.stereotype.Component

@Component
class EnvironmentSchedulerWorkerFactoryVisitor(
  private val schedulingConfigActivities: SchedulingConfigActivities,
  private val actuatorActivities: ActuatorActivities,
  private val supervisorActivities: SupervisorActivities
) : AbstractWorkerFactoryVisitor() {

  override val name: String = "environment-scheduler"

  override fun accept(workerFactory: WorkerFactory, workerOptions: WorkerOptions, taskQueueNamer: TaskQueueNamer) {
    workerFactory.newWorker(taskQueueNamer.name(ENVIRONMENT_SCHEDULER_TASK_QUEUE)).apply {
      registerWorkflowImplementationTypes(EnvironmentSchedulerImpl::class.java)
      registerWorkflowImplementationTypes(DefaultSchedulerSupervisor::class.java)
      registerActivitiesImplementations(schedulingConfigActivities, actuatorActivities, supervisorActivities, taskQueueNamer)
    }
  }

  override fun onStart(workflowClient: WorkflowClient, taskQueueNamer: TaskQueueNamer) {
    startSupervisor(SupervisorType.ENVIRONMENT, workflowClient, taskQueueNamer)
  }
}
