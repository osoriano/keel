package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.scheduling.ResourceScheduler.* // ktlint-disable no-wildcard-imports
import com.netflix.spinnaker.keel.telemetry.ResourceCheckStarted
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
import io.temporal.api.workflowservice.v1.SignalWorkflowExecutionRequest
import io.temporal.api.workflowservice.v1.TerminateWorkflowExecutionRequest
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
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
      log.debug("Unable to temporal schedule resource ${resource.id} in application ${resource.application} because it's not in the allow list")
      return
    }
    log.debug("Temporal scheduling resource ${resource.id} in application ${resource.application}")
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
      log.debug("Unable to stop temporal scheduling resource ${resource.id} in application ${resource.application} because it's not in the allow list")
      return
    }
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

  /**
   * Supports dual-scheduling modes in Keel while transitioning away from the old checker scheduling strategy to
   * Temporal. This allows us to JIT schedule or unschedule resources on the Temporal path in response to FP toggles.
   *
   * Once Temporal is used for all scheduling, this event listener can disappear entirely. At this point, we can maybe
   * create a supervisor workflow that scans the database verifying all workflows that need to exist actually do.
   */
  @EventListener(ResourceCheckStarted::class)
  fun onResourceCheckStarted(event: ResourceCheckStarted) {
    if (environment.isTemporalSchedulingEnabled(event.resource)) {
      startScheduling(event.resource)
    } else {
      stopScheduling(event.resource)
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
    if (!environment.isTemporalSchedulingEnabled(resource)) {
      return
    }

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
