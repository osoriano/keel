package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.config.FeatureToggles.Companion.SUPERVISOR_SCHEDULING_CONFIG
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.persistence.ResourceHeader
import com.netflix.spinnaker.keel.scheduling.ResourceScheduler.* // ktlint-disable no-wildcard-imports
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.RESOURCE_SCHEDULER_TASK_QUEUE
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.TEMPORAL_NAMESPACE
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.WORKER_ENV_SEARCH_ATTRIBUTE
import com.netflix.spinnaker.keel.telemetry.ResourceAboutToBeChecked
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
import org.springframework.context.event.EventListener
import org.springframework.core.env.Environment
import org.springframework.stereotype.Service

@Service
class ResourceSchedulerService(
  private val workflowClientProvider: WorkflowClientProvider,
  private val workflowServiceStubsProvider: WorkflowServiceStubsProvider,
  private val taskQueueNamer: TaskQueueNamer,
  private val environment: Environment,
  private val workerEnvironment: WorkerEnvironment,
  private val featureToggles: FeatureToggles
) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  fun isScheduling(application: String): Boolean {
    return environment.isTemporalSchedulingEnabled(application)
  }

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
    if (!environment.isTemporalSchedulingEnabled(resource)) {
      log.debug("Unable to temporal schedule resource ${resource.id} in application ${resource.application} because it's not in the allow list")
      return
    }

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
            WORKER_ENV_SEARCH_ATTRIBUTE to workerEnvironment.get().name
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

  /**
   * Supports dual-scheduling modes in Keel while transitioning away from the old checker scheduling strategy to
   * Temporal. This allows us to JIT schedule or unschedule resources on the Temporal path in response to FP toggles.
   *
   * Once Temporal is used for all scheduling, this event listener can disappear entirely. At this point, we can maybe
   * create a supervisor workflow that scans the database verifying all workflows that need to exist actually do.
   */
  @EventListener(ResourceAboutToBeChecked::class)
  fun onResourceCheckStarted(event: ResourceAboutToBeChecked) {
    if (featureToggles.isEnabled(SUPERVISOR_SCHEDULING_CONFIG, false)) {
      // new scheduling stuff will kick in, we don't need to start and stop scheduling this way.
      // todo eb: remove once confident in the new scheduling.
      return
    }

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

  private val ResourceHeader.workflowExecution
    get() = WorkflowExecution.newBuilder()
      .setWorkflowId(workflowId(uid))
      .build()

  private fun workflowId(uid: String): String = "resource:$uid"

  private val Resource<*>.workflowId
    get() = workflowId(header.uid)

  private fun Environment.isTemporalSchedulingEnabled(application: String): Boolean {
    if (environment.getProperty(ENABLED_GLOBALLY_CONFIG, Boolean::class.java, false)) {
      return true
    }

    val allowedApplications = environment.getProperty(ALLOWED_APPS_CONFIG, String::class.java, "").split(',')
    return allowedApplications.contains(application)
  }

  private fun Environment.isTemporalSchedulingEnabled(resource: ResourceHeader): Boolean =
    isTemporalSchedulingEnabled(resource.application)

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
