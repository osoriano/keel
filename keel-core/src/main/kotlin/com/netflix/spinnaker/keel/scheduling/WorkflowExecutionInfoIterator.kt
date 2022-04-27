package com.netflix.spinnaker.keel.scheduling

import com.google.protobuf.ByteString
import io.temporal.api.workflow.v1.WorkflowExecutionInfo
import io.temporal.api.workflowservice.v1.ListWorkflowExecutionsRequest
import io.temporal.serviceclient.WorkflowServiceStubs

/**
 * Iterates paginated list workflow execution requests.
 */
class WorkflowExecutionInfoIterator(
  private val workflowServiceStubs: WorkflowServiceStubs,
  private val requestPrototype: ListWorkflowExecutionsRequest,
  private val pageSize: Int = 100
) : Iterator<WorkflowExecutionInfo> {

  private lateinit var iterator: Iterator<WorkflowExecutionInfo>
  private var nextPageToken: ByteString? = null

  init {
    doRequest()
  }

  override fun hasNext(): Boolean {
    if (iterator.hasNext()) {
      return true
    }
    if (nextPageToken != null) {
      doRequest()
      return iterator.hasNext()
    }
    return false
  }

  override fun next(): WorkflowExecutionInfo {
    return iterator.next()
  }

  private fun doRequest() {
    val request = ListWorkflowExecutionsRequest.newBuilder(requestPrototype)
    request.pageSize = pageSize
    if (nextPageToken != null) {
      request.nextPageToken = nextPageToken
    }
    val response = workflowServiceStubs.blockingStub().listWorkflowExecutions(request.build())
    nextPageToken = response.nextPageToken
    iterator = response.executionsList.iterator()
  }
}
