package com.netflix.spinnaker.keel.titus

import com.netflix.spinnaker.keel.orca.ExecutionDetailResponse

/**
 * Links to an orca task
 */
class OrcaLinkStrategy(
  private val baseUrl: String
) : LinkStrategy {

  /**
   * $baseUrl/#/applications/APP/tasks/TASKID
   */
  override fun url(stageExecution: ExecutionDetailResponse) =
    "$baseUrl/#/applications/${stageExecution.application}/tasks/${stageExecution.id}"

  override fun url(jobStatus: Map<String, Any?>): String? {
    TODO("not implemented")
  }
}