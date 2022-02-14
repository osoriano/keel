package com.netflix.spinnaker.keel.buoy

import com.netflix.buoy.sdk.RolloutWorkflow
import com.netflix.buoy.sdk.model.RolloutTarget
import com.netflix.temporal.spring.WorkflowClientProvider
import io.temporal.client.WorkflowClient
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.springframework.stereotype.Component

/**
 * Temporal-based client for talking to Buoy.
 */
@Component
class BuoyClient(
  private val workflowClientProvider: WorkflowClientProvider
) {
  suspend fun deployImmediately(workflowId: String, rolloutTarget: RolloutTarget) {
    coroutineScope {
      launch(IO) {
        workflowClientProvider
          .get("spinnaker-tools")
          .newWorkflowStub<RolloutWorkflow>(workflowId)
          .deployImmediately(rolloutTarget)
      }
    }
  }
}

private inline fun <reified T> WorkflowClient.newWorkflowStub(workflowId: String): T =
  newWorkflowStub(T::class.java, workflowId)
