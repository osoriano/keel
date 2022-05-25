package com.netflix.spinnaker.keel.scheduling

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.logging.blankMDC
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.TEMPORAL_NAMESPACE
import com.netflix.temporal.spring.WorkflowServiceStubsProvider
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.ResetReapplyType
import io.temporal.api.workflowservice.v1.ResetWorkflowExecutionRequest
import io.temporal.api.workflowservice.v1.TerminateWorkflowExecutionRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Clock
import java.util.UUID
import kotlin.coroutines.CoroutineContext

/**
 * An admin service for temporal operations. These are api-triggered and are used to handle operational oopsies.
 */
@Service
class TemporalAdminService(
  private val repository: KeelRepository,
  private val temporalSchedulerService: TemporalSchedulerService,
  private val workflowServiceStubsProvider: WorkflowServiceStubsProvider,
  private val clock: Clock,
  private val registry: Registry
): CoroutineScope {
  override val coroutineContext: CoroutineContext = Dispatchers.Default

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  /**
   * Check now makes the wf continue as new, which will help us switch versions.
   */
  fun checkAllResourcesNow() {
    var i = 0
    repository.allResources().forEachRemaining {
      temporalSchedulerService.checkResourceNow(it)
      registry.counter("keel.temporal-admin.resource.check-now.triggered").increment()
      maybeLog(i++, "Checking all resources")
    }
  }

  /**
   * Check now makes the wf continue as new, which will help us switch versions.
   */
  fun checkAllEnvironmentsNow() {
    var i = 0
    repository.allEnvironments().forEachRemaining {
      temporalSchedulerService.checkEnvironmentNow(it.application, it.name)
      registry.counter("keel.temporal-admin.environment.check-now.triggered").increment()
      maybeLog(i++, "Checking all environments")
    }
  }

  fun resetAllResourceWorkflowsAsync() {
    launch(blankMDC) {
      resetAllResourceWorkflows()
    }
  }

  fun resetAllEnvironmentWorkflowsAsync() {
    launch(blankMDC) {
      resetAllEnvironmentWorkflows()
    }
  }

  suspend fun resetAllResourceWorkflows() {
    var i = 0
    repository.allResources().forEachRemaining {
      maybeLog(i++, "Resetting all resource workflows")
      resetHistory(temporalSchedulerService.workflowId(it.uid))
      registry.counter("keel.temporal-admin.resource.history-reset.count").increment()
    }
  }

  suspend fun resetAllEnvironmentWorkflows() {
    var i = 0
    repository.allEnvironments().forEachRemaining {
      maybeLog(i++, "Resetting all environment workflows")
      resetHistory(temporalSchedulerService.workflowId(it.application, it.name))
      registry.counter("keel.temporal-admin.environment.history-reset.triggered").increment()
    }
  }

  fun terminateAllResourceWorkflowsAsync() {
    launch(blankMDC) {
      terminateAllResourceWorkflows()
    }
  }

  fun terminateAllEnvironmentWorkflowsAsync() {
    launch(blankMDC) {
      terminateAllEnvironmentWorkflows()
    }
  }

  suspend fun terminateAllResourceWorkflows() {
    var i = 0
    repository.allResources().forEachRemaining {
      maybeLog(i++, "Terminating all resource workflows")
      terminate(temporalSchedulerService.workflowId(it.uid))
      registry.counter("keel.temporal-admin.resource.terminate-workflow.count").increment()
    }
  }

  suspend fun terminateAllEnvironmentWorkflows() {
    var i = 0
    repository.allEnvironments().forEachRemaining {
      maybeLog(i++, "Terminating all environment workflows")
      terminate(temporalSchedulerService.workflowId(it.application, it.name))
      registry.counter("keel.temporal-admin.environment.terminate-workflow.triggered").increment()
    }
  }

  fun resetHistory(id: String) {
    try {
      workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE)
        .blockingStub()
        .resetWorkflowExecution(
          ResetWorkflowExecutionRequest.newBuilder()
            .setWorkflowExecution(WorkflowExecution.newBuilder()
              .setWorkflowId(id)
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
        log.error("Failed to reset workflow execution for $id", e)
      }
      // not found will be handled by the supervisor process
    }
  }

  fun terminate(id: String) {
    try {
      workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE)
        .blockingStub()
        .terminateWorkflowExecution(
          TerminateWorkflowExecutionRequest.newBuilder()
            .setWorkflowExecution(WorkflowExecution.newBuilder()
              .setWorkflowId(id)
              .build())
            .setNamespace(TEMPORAL_NAMESPACE)
            .setReason("Terminating because of user triggered request at ${clock.instant()}")
            .build()
        )
    } catch (e: StatusRuntimeException) {
      if (e.status.code != Status.Code.NOT_FOUND) {
        log.error("Failed to reset workflow execution for $id", e)
      }
      // not found will be handled by the supervisor process
    }
  }

  fun maybeLog(i: Int, message: String) {
    if (i % 10 == 0) {
      log.info("$message: on iteration $i")
    }
  }

  private fun workflowExecution(id: String) =
    WorkflowExecution.newBuilder()
      .setWorkflowId(id)
      .build()
}
