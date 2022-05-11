package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.scheduling.SchedulerSupervisor.SupervisorType
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.ENVIRONMENT_SCHEDULER_TASK_QUEUE
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.RESOURCE_SCHEDULER_TASK_QUEUE
import com.netflix.temporal.core.WorkerFactoryVisitor
import com.netflix.temporal.core.convention.TaskQueueNamer
import com.netflix.temporal.spring.convention.LaptopTaskQueueNamer
import io.temporal.api.enums.v1.WorkflowIdReusePolicy
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowExecutionAlreadyStarted
import io.temporal.client.WorkflowOptions
import io.temporal.common.RetryOptions
import org.slf4j.LoggerFactory
import java.time.Duration

abstract class AbstractWorkerFactoryVisitor : WorkerFactoryVisitor {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  /**
   * Each keel instance will run this on startup, attempting to create the same SchedulerSupervisor workflow.
   * Since the workflow ID is static, only one will ever be running at any given time.
   */
  protected fun startSupervisor(
    type: SupervisorType,
    workflowClient: WorkflowClient,
    taskQueueNamer: TaskQueueNamer
  ) {
    val taskQueue = when (type) {
      SupervisorType.ENVIRONMENT -> ENVIRONMENT_SCHEDULER_TASK_QUEUE
      SupervisorType.RESOURCE -> RESOURCE_SCHEDULER_TASK_QUEUE
    }
    val options = WorkflowOptions.newBuilder()
      .setWorkflowId(supervisorId(type, taskQueueNamer))
      .setTaskQueue(taskQueueNamer.name(taskQueue))
      .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY)
      .setRetryOptions(
        RetryOptions.newBuilder()
          .setInitialInterval(Duration.ofMinutes(1))
          .setBackoffCoefficient(1.0)
          .build()
      )
      .build()

    val workflow = workflowClient.newWorkflowStub(SchedulerSupervisor::class.java, options)
    try {
      WorkflowClient.start(workflow::supervise, SchedulerSupervisor.SuperviseRequest(type))
      log.info("Started {}", SchedulerSupervisor::class.java)
    } catch (e: WorkflowExecutionAlreadyStarted) {
      log.info("SchedulerSupervisor ${options.workflowId} already exists")
    }
  }

  /**
   * We want a supervisor created uniquely for laptop users. This is a little janky, but it allows us to name
   * supervisors after the person's laptop.
   */
  private fun supervisorId(type: SupervisorType, taskQueueNamer: TaskQueueNamer): String {
    val workflowId = when (type) {
      SupervisorType.RESOURCE -> SchedulingConsts.RESOURCE_SCHEDULER_SUPERVISOR_WORKFLOW_ID
      SupervisorType.ENVIRONMENT -> SchedulingConsts.ENVIRONMENT_SCHEDULER_SUPERVISOR_WORKFLOW_ID
    }

    return if (taskQueueNamer is LaptopTaskQueueNamer) {
      val name = taskQueueNamer.name("foo").split("/")[0]
      "$name-$workflowId"
    } else {
      workflowId
    }
  }
}
