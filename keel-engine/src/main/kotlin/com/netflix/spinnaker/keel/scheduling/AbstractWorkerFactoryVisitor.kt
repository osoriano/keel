package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.scheduling.SchedulerSupervisor.SupervisorType
import com.netflix.spinnaker.keel.scheduling.SchedulerSupervisor.SupervisorType.*
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.ENVIRONMENT_SCHEDULER_SUPERVISOR_WORKFLOW_ID
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.ENVIRONMENT_SCHEDULER_TASK_QUEUE
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.RESOURCE_SCHEDULER_SUPERVISOR_WORKFLOW_ID
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.RESOURCE_SCHEDULER_TASK_QUEUE
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.TEMPORAL_NAMESPACE
import com.netflix.temporal.core.WorkerFactoryVisitor
import com.netflix.temporal.core.convention.TaskQueueNamer
import com.netflix.temporal.spring.WorkflowServiceStubsProvider
import com.netflix.temporal.spring.convention.LaptopTaskQueueNamer
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.ResetReapplyType
import io.temporal.api.enums.v1.WorkflowIdReusePolicy
import io.temporal.api.workflowservice.v1.ResetWorkflowExecutionRequest
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowExecutionAlreadyStarted
import io.temporal.client.WorkflowOptions
import io.temporal.common.RetryOptions
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.time.Clock
import java.time.Duration
import java.util.UUID

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
      ENVIRONMENT -> ENVIRONMENT_SCHEDULER_TASK_QUEUE
      RESOURCE -> RESOURCE_SCHEDULER_TASK_QUEUE
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

  fun resetSupervisor(
    type: SupervisorType,
    workflowClient: WorkflowClient,
    taskQueueNamer: TaskQueueNamer,
    workflowServiceStubsProvider: WorkflowServiceStubsProvider,
    clock: Clock
  ) {
    log.info("Resetting temporal $type supervisor")
    try {
      workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE)
        .blockingStub()
        .resetWorkflowExecution(
          ResetWorkflowExecutionRequest.newBuilder()
            .setWorkflowExecution(WorkflowExecution.newBuilder()
              .setWorkflowId(supervisorId(type, taskQueueNamer))
              .build())
            .setNamespace(TEMPORAL_NAMESPACE)
            .setWorkflowTaskFinishEventId(3) // This is the first WORKFLOW_TASK_STARTED event ID
            .setResetReapplyType(ResetReapplyType.RESET_REAPPLY_TYPE_NONE)
            .setRequestId(UUID.randomUUID().toString())
            .setReason("Resetting history because of user triggered request at ${clock.instant()}")
            .build()
        )
    } catch (e: StatusRuntimeException) {
      if (e.status.code != Status.Code.NOT_FOUND) {
        log.error("Failed to reset workflow execution for $type", e)
      } else {
        log.info("Starting temporal $type supervisor because it wasn't found")
        startSupervisor(type, workflowClient, taskQueueNamer)
      }
    }
  }

  /**
   * We want a supervisor created uniquely for laptop users. This is a little janky, but it allows us to name
   * supervisors after the person's laptop.
   */
  fun supervisorId(type: SupervisorType, taskQueueNamer: TaskQueueNamer): String {
    val workflowId = when (type) {
      RESOURCE -> RESOURCE_SCHEDULER_SUPERVISOR_WORKFLOW_ID
      ENVIRONMENT -> ENVIRONMENT_SCHEDULER_SUPERVISOR_WORKFLOW_ID
    }

    return if (taskQueueNamer is LaptopTaskQueueNamer) {
      val name = taskQueueNamer.name("foo").split("/")[0]
      "$name-$workflowId"
    } else {
      workflowId
    }
  }
}
