package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.scheduling.ResourceScheduler.* // ktlint-disable no-wildcard-imports
import com.netflix.temporal.core.convention.TaskQueueNamer
import com.netflix.temporal.spring.WorkflowClientProvider
import com.netflix.temporal.spring.WorkflowServiceStubsProvider
import io.github.resilience4j.retry.annotation.Retry
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.enums.v1.WorkflowIdReusePolicy
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.api.workflowservice.v1.TerminateWorkflowExecutionRequest
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import org.slf4j.LoggerFactory
import org.springframework.core.env.Environment
import org.springframework.stereotype.Service

@Service
@Retry(name = "resource-scheduler")
class ResourceSchedulerService(
  private val workflowClientProvider: WorkflowClientProvider,
  private val workflowServiceStubsProvider: WorkflowServiceStubsProvider,
  private val taskQueueNamer: TaskQueueNamer,
  private val environment: Environment
) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  fun isScheduling(application: String): Boolean {
    return environment.isTemporalSchedulingEnabled(application)
  }

  fun isScheduling(resource: Resource<*>): Boolean {
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

  fun startScheduling(resource: Resource<*>) {
    if (!environment.isTemporalSchedulingEnabled(resource)) {
      return
    }
    val client = workflowClientProvider.get(TEMPORAL_NAMESPACE)

    val stub = client.newWorkflowStub(
      ResourceScheduler::class.java,
      WorkflowOptions.newBuilder()
        .setTaskQueue(taskQueueNamer.name(RESOURCE_SCHEDULER_TASK_QUEUE))
        .setWorkflowId(resource.workflowId)
        .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY)
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

  fun stopScheduling(resource: Resource<*>) {
    if (!environment.isTemporalSchedulingEnabled(resource)) {
      return
    }

    val stub = workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE)

    stub.blockingStub()
      .terminateWorkflowExecution(
        TerminateWorkflowExecutionRequest.newBuilder()
          .setNamespace(TEMPORAL_NAMESPACE)
          .setWorkflowExecution(resource.workflowExecution)
          .build()
      )
  }

  private fun getWorkflowExecution(resourceId: String): WorkflowExecution =
    WorkflowExecution.newBuilder()
      .setWorkflowId("resource:$resourceId")
      .build()

  private val Resource<*>.workflowExecution
    get() = WorkflowExecution.newBuilder()
      .setWorkflowId(workflowId)
      .build()

  private val Resource<*>.workflowId
    get() = "resource:${metadata["uid"]}"

  private fun Environment.isTemporalSchedulingEnabled(application: String): Boolean {
    if (environment.getProperty(ENABLED_GLOBALLY_CONFIG, Boolean::class.java, false)) {
      return true
    }

    val allowedApplications = environment.getProperty(ALLOWED_APPS_CONFIG, String::class.java, "").split(',')
    return allowedApplications.contains(application)
  }

  private fun Environment.isTemporalSchedulingEnabled(resource: Resource<*>): Boolean =
    isTemporalSchedulingEnabled(resource.application)

  private companion object {
    private const val ENABLED_GLOBALLY_CONFIG = "keel.resource-scheduler.enabled-globally"
    private const val ALLOWED_APPS_CONFIG = "keel.resource-scheduler.applications-allowed"

    private val EXECUTION_RUNNING_STATUSES = setOf(
      WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
      WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
    )
  }
}
