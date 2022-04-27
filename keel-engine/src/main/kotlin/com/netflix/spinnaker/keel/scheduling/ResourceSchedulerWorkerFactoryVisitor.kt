package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.RESOURCE_SCHEDULER_TASK_QUEUE
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.SCHEDULER_SUPERVISOR_WORKFLOW_ID
import com.netflix.spinnaker.keel.scheduling.activities.ActuatorActivities
import com.netflix.spinnaker.keel.scheduling.activities.SchedulingConfigActivities
import com.netflix.spinnaker.keel.scheduling.activities.SupervisorActivities
import com.netflix.temporal.core.WorkerFactoryVisitor
import com.netflix.temporal.core.convention.TaskQueueNamer
import com.netflix.temporal.spring.convention.LaptopTaskQueueNamer
import io.temporal.api.enums.v1.WorkflowIdReusePolicy
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowExecutionAlreadyStarted
import io.temporal.client.WorkflowOptions
import io.temporal.worker.WorkerFactory
import io.temporal.worker.WorkerOptions
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class ResourceSchedulerWorkerFactoryVisitor(
  private val schedulingConfigActivities: SchedulingConfigActivities,
  private val actuatorActivities: ActuatorActivities,
  private val supervisorActivities: SupervisorActivities
) : WorkerFactoryVisitor {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  override val name: String = "resource-scheduler"

  override fun accept(workerFactory: WorkerFactory, workerOptions: WorkerOptions, taskQueueNamer: TaskQueueNamer) {
    workerFactory.newWorker(taskQueueNamer.name(RESOURCE_SCHEDULER_TASK_QUEUE)).apply {
      registerWorkflowImplementationTypes(ResourceSchedulerImpl::class.java)
      registerWorkflowImplementationTypes(DefaultSchedulerSupervisor::class.java)
      registerActivitiesImplementations(schedulingConfigActivities, actuatorActivities, supervisorActivities, taskQueueNamer)
    }
  }

  override fun onStart(workflowClient: WorkflowClient, taskQueueNamer: TaskQueueNamer) {
    // Each keel instance will run this on startup, attempting to create the same SchedulerSupervisor workflow.
    // Since the workflow ID is static, only one will ever be running at any given time.
    val options = WorkflowOptions.newBuilder()
      .setWorkflowId(supervisorId(taskQueueNamer))
      .setTaskQueue(taskQueueNamer.name(RESOURCE_SCHEDULER_TASK_QUEUE))
      .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY)
      .build()

    val workflow = workflowClient.newWorkflowStub(SchedulerSupervisor::class.java, options)
    try {
      WorkflowClient.start(workflow::supervise, SchedulerSupervisor.SuperviseRequest())
      log.info("Started {}", SchedulerSupervisor::class.java)
    } catch (e: WorkflowExecutionAlreadyStarted) {
      // Do nothing
    }
  }

  /**
   * We want a supervisor created uniquely for laptop users. This is a little janky, but it allows us to name
   * supervisors after the person's laptop.
   */
  private fun supervisorId(taskQueueNamer: TaskQueueNamer): String {
    return if (taskQueueNamer is LaptopTaskQueueNamer) {
      val name = taskQueueNamer.name("foo").split("/")[0]
      "$name-$SCHEDULER_SUPERVISOR_WORKFLOW_ID"
    } else {
      SCHEDULER_SUPERVISOR_WORKFLOW_ID
    }
  }
}
