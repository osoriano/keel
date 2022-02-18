@file:Suppress("UNCHECKED_CAST")
package com.netflix.spinnaker.keel.verification.jenkins

import com.netflix.spinnaker.config.GitLinkConfig
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.TaskExecution
import com.netflix.spinnaker.keel.api.TaskStatus.NOT_STARTED
import com.netflix.spinnaker.keel.api.TaskStatus.RUNNING
import com.netflix.spinnaker.keel.api.TaskStatus.SUCCEEDED
import com.netflix.spinnaker.keel.api.action.ActionState
import com.netflix.spinnaker.keel.api.actuation.SubjectType.VERIFICATION
import com.netflix.spinnaker.keel.api.actuation.TaskLauncher
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.FAIL
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.NOT_EVALUATED
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PASS
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PENDING
import com.netflix.spinnaker.keel.api.plugins.VerificationEvaluator
import com.netflix.spinnaker.keel.model.OrcaJob
import com.netflix.spinnaker.keel.network.NetworkEndpointProvider
import com.netflix.spinnaker.keel.orca.ExecutionDetailResponse
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.verification.BaseVerificationEvaluator
import com.netflix.spinnaker.keel.verification.StandardTestParameter
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Instant

/**
 * A [VerificationEvaluator] that runs a Jenkins job to verify an environment.
 */
@Component
class JenkinsJobVerificationEvaluator(
  private val taskLauncher: TaskLauncher,
  override val keelRepository: KeelRepository,
  override val gitLinkConfig: GitLinkConfig,
  override val networkEndpointProvider: NetworkEndpointProvider
) : BaseVerificationEvaluator<JenkinsJobVerification>(keelRepository, gitLinkConfig, networkEndpointProvider) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  override val supportedVerification: Pair<String, Class<JenkinsJobVerification>> =
    JenkinsJobVerification.TYPE to JenkinsJobVerification::class.java

  override suspend fun evaluate(
    context: ArtifactInEnvironmentContext,
    verification: JenkinsJobVerification,
    oldState: ActionState
  ): ActionState {
    log.debug("Getting updated verification state from Jenkins job ${verification.name} for ${context.shortName()}")
    val taskExecution = taskLauncher.getTaskExecution(oldState.taskId)
    val actionState = with(taskExecution) {
      when {
        status.isSuccess() -> oldState.copy(status = PASS, endedAt = endTime ?: Instant.now())
        status.isIncomplete() -> oldState.copy(status = PENDING)
        else -> oldState.copy(status = FAIL, endedAt = endTime ?: Instant.now())
      }
    }
    log.debug("Current verification state from Jenkins job ${verification.name} for ${context.shortName()}: $actionState")
    return actionState.copy(link = taskExecution.jenkinsJobLink)
  }

  override suspend fun start(context: ArtifactInEnvironmentContext, verification: JenkinsJobVerification): Map<String, Any?> {
    val standardParameters = getStandardTestParameters(context)
    val processedParameters = standardParameters +
      verification.staticParameters +
      verification.dynamicParameters.render(standardParameters)

    val job = with(verification) {
      OrcaJob(
        type = "jenkins",
        mapOf(
          "master" to controller,
          "job" to job,
          "parameters" to processedParameters
        )
      )
    }

    with(context) {
      val description = "Verify $version in environment $environmentName with Jenkins job ${verification.name}"
      log.debug("$description: $job")
      val task = taskLauncher.submitJob(
        type = VERIFICATION,
        environmentName = environmentName,
        resourceId = null,
        description = description,
        user = deliveryConfig.serviceAccount,
        application = deliveryConfig.application,
        notifications = emptySet(),
        stages = listOf(job)
      )
      return mapOf("tasks" to listOf(task.id))
    }
  }

  private val ActionState.taskId: String
    get() = (metadata["tasks"] as? List<String>)?.first()
      ?: error("Task ID not found in verification metadata: $metadata")

  private fun Map<String, StandardTestParameter>.render(standardParameters: Map<String, Any>) =
    mapValues { (_, value) ->
      standardParameters[value.name] ?: "Unknown parameter $value"
    }

  private val TaskExecution.jenkinsContext: Map<String, Any?>
    get() = (this as? ExecutionDetailResponse)?.let {
      it.execution?.stages?.find { stage -> stage.type == "jenkins" }?.context
    } ?: emptyMap()

  private val TaskExecution.jenkinsJobLink: String?
    get() = (jenkinsContext["buildInfo"] as? Map<String, Any?>)?.get("url")?.toString()
}
