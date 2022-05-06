package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.persistence.ResourceHeader
import com.netflix.spinnaker.keel.scheduling.ResourceScheduler.* // ktlint-disable no-wildcard-imports
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.RESOURCE_SCHEDULER_TASK_QUEUE
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.TEMPORAL_NAMESPACE
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.WORKER_ENV_SEARCH_ATTRIBUTE
import com.netflix.temporal.core.convention.TaskQueueNamer
import com.netflix.temporal.spring.WorkflowClientProvider
import com.netflix.temporal.spring.WorkflowServiceStubsProvider
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.enums.v1.WorkflowIdReusePolicy
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.api.workflowservice.v1.SignalWorkflowExecutionRequest
import io.temporal.api.workflowservice.v1.TerminateWorkflowExecutionRequest
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class ResourceSchedulerService(
  private val workflowClientProvider: WorkflowClientProvider,
  private val workflowServiceStubsProvider: WorkflowServiceStubsProvider,
  private val taskQueueNamer: TaskQueueNamer,
  private val workerEnvironment: WorkerEnvironment
) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  /**
   * Checks for a running workflow for the resource
   */
  fun isScheduling(resource: Resource<*>): Boolean {
    return isScheduling(ResourceHeader(resource.id, resource.kind, resource.header.uid, resource.application))
  }

  /**
   * Checks for a running workflow for the resource
   */
  fun isScheduling(resource: ResourceHeader): Boolean {
    val client = workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE)

    val wf = try {
      client.blockingStub()
        .describeWorkflowExecution(
          DescribeWorkflowExecutionRequest.newBuilder()
            .setNamespace(TEMPORAL_NAMESPACE)
            .setExecution(resource.workflowExecution)
            .build()
        )
    } catch (e: StatusRuntimeException) {
      if (e.status.code != Status.Code.NOT_FOUND) {
        log.error("Failed to describe workflow execution", e)
      }
      return false
    }

    return EXECUTION_RUNNING_STATUSES.contains(wf.workflowExecutionInfo.status)
  }

  fun startScheduling(resource: ResourceHeader) {
    if (isScheduling(resource)) {
      // resource is already scheduled
      return
    }

    log.debug("Temporal scheduling resource ${resource.id} in application ${resource.application}")
    val client = workflowClientProvider.get(TEMPORAL_NAMESPACE)

    val stub = client.newWorkflowStub(
      ResourceScheduler::class.java,
      WorkflowOptions.newBuilder()
        .setTaskQueue(taskQueueNamer.name(RESOURCE_SCHEDULER_TASK_QUEUE))
        .setWorkflowId(workflowId(resource.uid))
        .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY)
        .setSearchAttributes(
          mapOf(
            WORKER_ENV_SEARCH_ATTRIBUTE to workerEnvironment.get()
          )
        )
        .build()
    )

    WorkflowClient.execute {
      stub.schedule(
        ScheduleResourceRequest(
          resourceId = resource.id,
          resourceKind = resource.kind.toString()
        )
      )
    }
  }

  fun startScheduling(resource: Resource<*>) {
    startScheduling(ResourceHeader(resource.id, resource.kind, resource.header.uid, resource.application))
  }

  fun stopScheduling(resource: Resource<*>) {
    log.debug("Removing Temporal scheduling of resource ${resource.id} in application ${resource.application}")

    val stub = workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE)

    try {
      stub.blockingStub()
        .terminateWorkflowExecution(
          TerminateWorkflowExecutionRequest.newBuilder()
            .setNamespace(TEMPORAL_NAMESPACE)
            .setWorkflowExecution(resource.workflowExecution)
            .build()
        )
    } catch (e: StatusRuntimeException) {
      if (e.status.code != Status.Code.NOT_FOUND) {
        throw e
      }
    }
  }

  fun checkNow(deliveryConfig: DeliveryConfig, environmentName: String? = null) {
    log.debug("Rechecking resources in application ${deliveryConfig.application} and environment ${environmentName ?: "all"}")
    deliveryConfig
      .environments.filter { environmentName == null || it.name == environmentName }
      .forEach { environment ->
        environment.resources.forEach { resource ->
          checkNow(resource)
        }
      }
  }

  fun checkNow(resource: Resource<*>) {
    val stub = workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE)
    log.debug("Rechecking resource ${resource.id} with workflow id ${resource.workflowId} in application ${resource.application}")
    try {
      stub.blockingStub()
        .signalWorkflowExecution(
          SignalWorkflowExecutionRequest.newBuilder()
            .setNamespace(TEMPORAL_NAMESPACE)
            .setWorkflowExecution(resource.workflowExecution)
            .setSignalName("checkNow")
            .build()
        )
    } catch (e: StatusRuntimeException) {
      if (e.status.code != Status.Code.NOT_FOUND) {
        log.error("Failed to describe workflow execution, starting scheduling", e)
        startScheduling(resource)
      }
    }
  }

  private fun getWorkflowExecution(resourceId: String): WorkflowExecution =
    WorkflowExecution.newBuilder()
      .setWorkflowId("resource:$resourceId")
      .build()

  private val Resource<*>.workflowExecution
    get() = WorkflowExecution.newBuilder()
      .setWorkflowId(workflowId)
      .build()

  private val ResourceHeader.workflowExecution
    get() = WorkflowExecution.newBuilder()
      .setWorkflowId(workflowId(uid))
      .build()

  private fun workflowId(uid: String): String = "resource:$uid"

  private val Resource<*>.workflowId
    get() = workflowId(header.uid)

  private companion object {
    private val EXECUTION_RUNNING_STATUSES = setOf(
      WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
      WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
    )
  }
}
