package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.config.FeatureToggles.Companion.TEMPORAL_ENV_CHECKING
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.persistence.ResourceHeader
import com.netflix.spinnaker.keel.scheduling.ResourceScheduler.* // ktlint-disable no-wildcard-imports
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.ENVIRONMENT_SCHEDULER_TASK_QUEUE
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
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service

@Service
class TemporalSchedulerService(
  private val workflowClientProvider: WorkflowClientProvider,
  private val workflowServiceStubsProvider: WorkflowServiceStubsProvider,
  private val taskQueueNamer: TaskQueueNamer,
  private val workerEnvironment: WorkerEnvironment,
  private val featureToggles: FeatureToggles
) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  fun environmentSchedulingEnabled(): Boolean =
    featureToggles.isEnabled(TEMPORAL_ENV_CHECKING, default = false)

  /**
   * Checks for a running workflow for the environment
   */
  fun isScheduling(application: String, environment: String): Boolean {
    val client = workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE)

    val wf = try {
      client.blockingStub()
        .describeWorkflowExecution(
          DescribeWorkflowExecutionRequest.newBuilder()
            .setNamespace(TEMPORAL_NAMESPACE)
            .setExecution(getWorkflowExecution(application, environment))
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

  fun startSchedulingEnvironment(application: String, environment: String) {
    if (!environmentSchedulingEnabled() || isScheduling(application, environment)) {
      // environment is already scheduled or scheduling is disabled
      return
    }

    log.debug("Temporal scheduling environment $environment in application $application")
    val client = workflowClientProvider.get(TEMPORAL_NAMESPACE)

    val stub = client.newWorkflowStub(
      EnvironmentScheduler::class.java,
      WorkflowOptions.newBuilder()
        .setTaskQueue(taskQueueNamer.name(ENVIRONMENT_SCHEDULER_TASK_QUEUE))
        .setWorkflowId(workflowId(application, environment))
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
        EnvironmentScheduler.ScheduleEnvironmentRequest(
          application = application,
          environment = environment
        )
      )
    }
  }

  fun stopScheduling(application: String, environment: String) {
    log.debug("Removing Temporal scheduling of environment $environment in application $application")

    val stub = workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE)

    try {
      stub.blockingStub()
        .terminateWorkflowExecution(
          TerminateWorkflowExecutionRequest.newBuilder()
            .setNamespace(TEMPORAL_NAMESPACE)
            .setWorkflowExecution(getWorkflowExecution(application, environment))
            .build()
        )
    } catch (e: StatusRuntimeException) {
      if (e.status.code != Status.Code.NOT_FOUND) {
        throw e
      }
    }
  }

  @EventListener(StopSchedulingEnvironmentEvent::class)
  fun onStopSchedulingEnvEvent(event: StopSchedulingEnvironmentEvent) {
    log.debug("Removing Temporal scheduling of environment ${event.environment} in application ${event.application} because of a fp switch")
    stopScheduling(event.application, event.environment)
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

  fun checkEnvironmentNow(application: String, environment: String) {
    if (!environmentSchedulingEnabled()) {
      return
    }
    log.debug("Rechecking environment $environment in application $application")
    val stub = workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE)
    try {
      stub.blockingStub()
        .signalWorkflowExecution(
          SignalWorkflowExecutionRequest.newBuilder()
            .setNamespace(TEMPORAL_NAMESPACE)
            .setWorkflowExecution(getWorkflowExecution(application, environment))
            .setSignalName("checkNow")
            .build()
        )
    } catch (e: StatusRuntimeException) {
      if (e.status.code != Status.Code.NOT_FOUND) {
        log.error("Failed to describe workflow execution, starting scheduling", e)
        startSchedulingEnvironment(application, environment)
      }
    }
  }

  fun checkResourcesInEnvironmentNow(deliveryConfig: DeliveryConfig, environmentName: String) {
    log.debug("Rechecking resources in application ${deliveryConfig.application} and environment $environmentName")
    deliveryConfig
      .environments.filter { it.name == environmentName }
      .forEach { environment ->
        environment.resources.forEach { resource ->
          checkResourceNow(resource)
        }
      }
  }

  fun checkResourceNow(resource: Resource<*>) {
    checkResourceNow(resource.header)
  }

  fun checkResourceNow(resource: ResourceHeader) {
    val stub = workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE)
    log.debug("Rechecking resource ${resource.id} with workflow id ${workflowId(resource.uid)} in application ${resource.application}")
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

  private fun getWorkflowExecution(application: String, environment: String) =
    WorkflowExecution.newBuilder()
      .setWorkflowId(workflowId(application, environment))
      .build()

  fun workflowId(application: String, environment: String) =
    "environment:$application:$environment"

  fun workflowId(uid: String): String = "resource:$uid"

  private val Resource<*>.workflowId
    get() = workflowId(header.uid)

  private companion object {
    private val EXECUTION_RUNNING_STATUSES = setOf(
      WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
      WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
    )
  }
}

// todo eb: remove once scheduling is switched over
data class StopSchedulingEnvironmentEvent(
  val application: String,
  val environment: String
)
