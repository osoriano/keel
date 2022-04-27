package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.TEMPORAL_NAMESPACE
import com.netflix.temporal.spring.WorkflowServiceStubsProvider
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.workflow.v1.WorkflowExecutionInfo
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.api.workflowservice.v1.ListWorkflowExecutionsRequest
import io.temporal.api.workflowservice.v1.TerminateWorkflowExecutionRequest
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class TemporalClient(
  private val workflowServiceStubsProvider: WorkflowServiceStubsProvider
) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  fun iterateWorkflows(prototype: ListWorkflowExecutionsRequest, pageSize: Int = 100): Iterator<WorkflowExecutionInfo> {
    val stub = workflowServiceStubsProvider.forNamespace(
      prototype.namespace ?: throw IllegalStateException("namespace must be set on ListWorkflowExecutionsRequest")
    )
    return WorkflowExecutionInfoIterator(stub, prototype, pageSize)
  }

  fun terminateWorkflow(namespace: String, workflowExecution: WorkflowExecution) {
    try {
      workflowServiceStubsProvider.forNamespace(namespace)
        .blockingStub()
        .terminateWorkflowExecution(
          TerminateWorkflowExecutionRequest.newBuilder()
            .setNamespace(TEMPORAL_NAMESPACE)
            .setWorkflowExecution(workflowExecution)
            .build()
        )
    } catch (e: StatusRuntimeException) {
      if (e.status != Status.NOT_FOUND) {
        throw e
      }
    }
  }

  fun workflowIsRunning(namespace: String, workflowId: String): Boolean {
    val wf = try {
      workflowServiceStubsProvider.forNamespace(namespace).blockingStub()
        .describeWorkflowExecution(
          DescribeWorkflowExecutionRequest.newBuilder()
            .setNamespace(TEMPORAL_NAMESPACE)
            .setExecution(WorkflowExecution.newBuilder()
              .setWorkflowId(workflowId)
              .build())
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

  private val EXECUTION_RUNNING_STATUSES = setOf(
    WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
    WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
  )
}
