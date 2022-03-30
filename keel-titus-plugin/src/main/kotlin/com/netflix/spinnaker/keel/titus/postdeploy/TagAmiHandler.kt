package com.netflix.spinnaker.keel.titus.postdeploy

import com.google.common.hash.Hashing
import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.config.BaseUrlConfig
import com.netflix.spinnaker.config.DefaultWorkhorseCoroutineContext
import com.netflix.spinnaker.config.WorkhorseCoroutineContext
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.action.ActionState
import com.netflix.spinnaker.keel.api.actuation.TaskLauncher
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.FAIL
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PASS
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PENDING
import com.netflix.spinnaker.keel.api.plugins.CurrentImages
import com.netflix.spinnaker.keel.api.plugins.PostDeployActionHandler
import com.netflix.spinnaker.keel.api.postdeploy.PostDeployAction
import com.netflix.spinnaker.keel.api.postdeploy.SupportedPostDeployActionType
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.core.api.DEFAULT_SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.core.api.TagAmiPostDeployAction
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.titus.OrcaLinkStrategy
import com.netflix.spinnaker.keel.titus.TITUS_JOB_TASKS
import com.netflix.spinnaker.keel.verification.ImageFinder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets
import java.time.Instant

/**
 * This class tags images after they've been verified, if they have the
 * tag-ami post deploy action
 *
 * This is meant to provide a hook to transition from managed
 * delivery back to pipelines. Images will be tagged with `latest tested = true`.
 * This allows a pipeline to find the latest tested image and deploy that.
 *
 * This should replace [ImageTagger] once we've verified that this is the right way to move forward.
 */
@Component
class TagAmiHandler(
  override val eventPublisher: EventPublisher,
  private val taskLauncher: TaskLauncher,
  private val orca: OrcaService,
  private val spectator: Registry,
  private val baseUrlConfig: BaseUrlConfig,
  private val imageFinder: ImageFinder,
  override val coroutineContext: WorkhorseCoroutineContext = DefaultWorkhorseCoroutineContext
) : PostDeployActionHandler<TagAmiPostDeployAction>, CoroutineScope {

  private val TAG_AMI_JOB_LAUNCHED = "keel.image.tag"

  override val supportedType = SupportedPostDeployActionType<TagAmiPostDeployAction>("tag-ami")

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  override suspend fun start(context: ArtifactInEnvironmentContext, action: PostDeployAction): Map<String, Any?> {
    log.debug("Starting tag-ami process for ${context.shortName()}")

    /**
     * The finder retrieves the images that are currently running in the environment
     */
    val images =  imageFinder.getImages(context.deliveryConfig, context.environmentName)
    log.debug("Found the following images for ${context.shortName()}: $images")

    val jobs = images
      .filter { it.kind.group == "ec2" } // spinnaker only supports tagging amis
      .map { it.toJob(context.environmentName) }

    val tasksIds = mutableListOf<String>()
    log.debug("Launching ${jobs.size} tasks to tag images for ${context.shortName()}")
    jobs.forEach { job ->
      val names = job["imageNames"].toString()
      val task = runBlocking {
        taskLauncher.submitJob(
          user = DEFAULT_SERVICE_ACCOUNT,
          application = context.deliveryConfig.application,
          environmentName = context.environmentName,
          resourceId = null,
          notifications = emptySet(),
          description = "Automatically tagging image(s) as verified $names",
          correlationId = hash(names), // hash guarantees this won't exceed 255 chars, otherwise orca returns a 500
          stages = listOf(job)
        )
      }
      log.debug("Launching task ${task.id} to tag image(s) $names")
      spectator.counter(
        TAG_AMI_JOB_LAUNCHED,
        listOf(BasicTag("application", context.deliveryConfig.application))
      ).increment()
      tasksIds.add(task.id)
    }

    return mapOf(TITUS_JOB_TASKS to tasksIds)
  }

  /**
   * Generate a 128-bit (32 hexadecimal characters) hash of a string of arbitrary length
   *
   * Example output: "02b6a6ab9b634d31e8adb0daa0922520"
   */
  @Suppress("UnstableApiUsage") // Hashing class is currently marked as beta
  private fun hash(s: String) =
    Hashing
      .murmur3_128()
      .hashString(s, StandardCharsets.UTF_8)
      .toString()

  /**
   * Checks if the orchestration execution associated with the tag ami  has completed.
   *
   * Precondition: the
   *
   *   [oldState] metadata must contain:
   *     key: "tasks"
   *     value: list where the last element is a valid orca task id
   *
   */
  override suspend fun evaluate(
    context: ArtifactInEnvironmentContext,
    action: PostDeployAction,
    oldState: ActionState
  ): ActionState {
    @Suppress("UNCHECKED_CAST")
    val taskId = (oldState.metadata[TITUS_JOB_TASKS] as? Iterable<String>)?.lastOrNull()

    if (taskId == null) {
      log.debug("Completing post deploy action ${context.shortName()} because no tasks were launched. Are there any ec2 clusters in the config?")
      return oldState.copy(status = FAIL, endedAt = Instant.now())
    }

    val response = orca.getOrchestrationExecution(taskId)

    log.debug("Container test task $taskId status: ${response.status.name}")

    val status = when {
      response.status.isSuccess() -> PASS
      response.status.isIncomplete() -> PENDING
      else -> FAIL
    }

    return oldState.copy(
      status = status,
      link = OrcaLinkStrategy(baseUrlConfig.baseUrl).url(response),
      endedAt = if(status==PENDING) null else Instant.now()
    )
  }

  fun CurrentImages.toJob(env: String): Map<String, Any?> =
    mapOf(
      "type" to "upsertImageTags",
      "imageNames" to images.map { it.imageName },
      "regions" to images.map { it.region }.toSet(),
      "tags" to mapOf(
        "latest tested" to true,
        env to "environment:passed"
      )
    )
}
