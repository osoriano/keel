package com.netflix.spinnaker.keel.actuation

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.config.ArtifactVersionCleanupConfig
import com.netflix.spinnaker.config.EnvironmentDeletionConfig
import com.netflix.spinnaker.config.EnvironmentVerificationConfig
import com.netflix.spinnaker.config.PostDeployActionsConfig
import com.netflix.spinnaker.config.ResourceCheckConfig
import com.netflix.spinnaker.config.TaskCheckConfig
import com.netflix.spinnaker.keel.activation.ApplicationDown
import com.netflix.spinnaker.keel.activation.ApplicationUp
import com.netflix.spinnaker.keel.exceptions.EnvironmentCurrentlyBeingActedOn
import com.netflix.spinnaker.keel.logging.TracingSupport.Companion.blankMDC
import com.netflix.spinnaker.keel.persistence.EnvironmentDeletionRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.TaskTrackingRepository
import com.netflix.spinnaker.keel.postdeploy.PostDeployActionRunner
import com.netflix.spinnaker.keel.scheduled.TaskActuator
import com.netflix.spinnaker.keel.telemetry.recordDurationPercentile
import com.netflix.spinnaker.keel.telemetry.safeIncrement
import com.netflix.spinnaker.keel.verification.VerificationRunner
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.core.env.Environment
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext
import kotlin.math.max

@EnableConfigurationProperties(
  ArtifactVersionCleanupConfig::class,
  ResourceCheckConfig::class,
  TaskCheckConfig::class,
  EnvironmentDeletionConfig::class,
  EnvironmentVerificationConfig::class,
  PostDeployActionsConfig::class,
)
@Component
class CheckScheduler(
  private val repository: KeelRepository,
  private val environmentDeletionRepository: EnvironmentDeletionRepository,
  private val resourceActuator: ResourceActuator,
  private val environmentPromotionChecker: EnvironmentPromotionChecker,
  private val verificationRunner: VerificationRunner,
  private val artifactHandlers: Collection<ArtifactHandler>,
  private val postDeployActionRunner: PostDeployActionRunner,
  private val artifactVersionCleanupConfig: ArtifactVersionCleanupConfig,
  private val resourceCheckConfig: ResourceCheckConfig,
  private val taskCheckConfig: TaskCheckConfig,
  private val verificationConfig: EnvironmentVerificationConfig,
  private val postDeployConfig: PostDeployActionsConfig,
  private val environmentDeletionConfig: EnvironmentDeletionConfig,
  private val environmentCleaner: EnvironmentCleaner,
  private val publisher: ApplicationEventPublisher,
  private val taskActuator: TaskActuator,
  private val taskTrackingRepository: TaskTrackingRepository,
  private val clock: Clock,
  private val springEnv: Environment,
  private val spectator: Registry
  ) : CoroutineScope {
  override val coroutineContext: CoroutineContext = Dispatchers.IO

  private val enabled = AtomicBoolean(false)

  @EventListener(ApplicationUp::class)
  fun onApplicationUp() {
    log.info("Application up, enabling scheduled resource checks")
    enabled.set(true)
  }

  @EventListener(ApplicationDown::class)
  fun onApplicationDown() {
    log.info("Application down, disabling scheduled resource checks")
    enabled.set(false)
  }

  // Used for resources, environments, and artifacts.
  private val checkMinAge: Duration
    get() = springEnv.getProperty("keel.check.min-age-duration", Duration::class.java, resourceCheckConfig.minAgeDuration)

  @Scheduled(fixedDelayString = "\${keel.resource-check.frequency:PT1S}")
  fun checkResources() {
    if (enabled.get()) {
      val startTime = clock.instant()
      val job = launch(blankMDC) {
        supervisorScope {
          runCatching {
            repository.resourcesDueForCheck(checkMinAge, resourceCheckConfig.batchSize)
          }
            .onFailure {
              log.error("Exception fetching resources due for check", it)
            }
            .onSuccess {
              log.info("Got resource batch({})", it.size)
              it.forEach {
                launch {
                  try {
                    withTimeout(resourceCheckConfig.timeoutDuration.toMillis()) {
                      resourceActuator.checkResource(it)
                    }
                  } catch (e: TimeoutCancellationException) {
                    log.error("Timed out checking resource ${it.id}", e)
                    spectator.counter(
                      "keel.scheduled.timeout",
                      listOf(BasicTag("type",  "resource"))
                    ).safeIncrement()
                  } catch (e: Exception ) {
                    log.error("Failure checking resource ${it.id}", e)
                    spectator.counter(
                      "keel.scheduled.failure",
                      listOf(BasicTag("type",  "resource"))
                    ).safeIncrement()
                  }
                }
              }
              spectator.counter(
                "keel.scheduled.batch.size",
                listOf(BasicTag("type", "resource"))
              ).increment(it.size.toLong())
            }
        }
      }
      runBlocking { job.join() }
      recordDuration(startTime, "resource")
    }
  }

  @Scheduled(fixedDelayString = "\${keel.environment-check.frequency:PT1S}")
  fun checkEnvironments() {
    if (enabled.get()) {
      val startTime = clock.instant()

      val job = launch(blankMDC) {
        supervisorScope {
          runCatching {
            repository.deliveryConfigsDueForCheck(checkMinAge, resourceCheckConfig.batchSize)
          }
            .onFailure {
              log.error("Exception fetching delivery configs due for check", it)
            }
            .onSuccess {
              it.forEach {
                launch {
                  try {
                    /**
                     * Sets the timeout to (checkTimeout * environmentCount), since a delivery-config's
                     * environments are checked sequentially within one coroutine job.
                     *
                     * TODO: consider refactoring environmentPromotionChecker so that it can be called for
                     *  individual environments, allowing fairer timeouts.
                     */
                    withTimeout(resourceCheckConfig.timeoutDuration.toMillis() * max(it.environments.size, 1)) {
                      environmentPromotionChecker.checkEnvironments(it)
                    }
                  } catch (e: TimeoutCancellationException) {
                    log.error("Timed out checking environments for ${it.application}/${it.name}", e)
                    spectator.counter(
                      "keel.scheduled.timeout",
                      listOf(BasicTag("type",  "environment"))
                    ).safeIncrement()
                  } catch (e: Exception) {
                    log.error("Failure checking environments for ${it.application}/${it.name}", e)
                    spectator.counter(
                      "keel.scheduled.failure",
                      listOf(BasicTag("type",  "environment"))
                    ).safeIncrement()
                  } finally {
                    repository.markDeliveryConfigCheckComplete(it)
                  }
                }
              }
              spectator.counter(
                "keel.scheduled.batch.size",
                listOf(BasicTag("type", "environment"))
              ).increment(it.size.toLong())
            }
        }
      }

      runBlocking { job.join() }
      recordDuration(startTime, "environment")
    }
  }

  @Scheduled(fixedDelayString = "\${keel.environment-deletion.check.frequency:PT1S}")
  fun checkEnvironmentsForDeletion() {
    if (enabled.get()) {
      val startTime = clock.instant()

      val job = launch(blankMDC) {
        supervisorScope {
          runCatching {
            environmentDeletionRepository.itemsDueForCheck(checkMinAge, resourceCheckConfig.batchSize)
          }
            .onFailure {
              log.error("Exception fetching environments due for deletion", it)
            }
            .onSuccess {
              it.forEach {
                launch {
                  try {
                    withTimeout(environmentDeletionConfig.check.timeoutDuration.toMillis()) {
                      environmentCleaner.cleanupEnvironment(it)
                    }
                  } catch (e: TimeoutCancellationException) {
                    log.error("Timed out checking environment ${it.name} for deletion", e)
                    spectator.counter(
                      "keel.scheduled.timeout",
                      listOf(BasicTag("type",  "environmentdeletion"))
                    ).safeIncrement()
                  } catch (e: Exception) {
                    log.error("Failed checking environment ${it.name} for deletion", e)
                    spectator.counter(
                      "keel.scheduled.failure",
                      listOf(BasicTag("type",  "environmentdeletion"))
                    ).safeIncrement()
                  }
                }
              }
              spectator.counter(
                "keel.scheduled.batch.size",
                listOf(BasicTag("type", "environmentdeletion"))
              ).increment(it.size.toLong())
            }
        }
      }

      runBlocking { job.join() }
      recordDuration(startTime, "environmentDeletion")
    }
  }

  @Scheduled(fixedDelayString = "\${keel.artifact-check.frequency:PT1S}")
  fun checkArtifacts() {
    if (enabled.get()) {
      val startTime = clock.instant()
      val job = launch(blankMDC) {
        supervisorScope {
          runCatching {
            repository.artifactsDueForCheck(checkMinAge, resourceCheckConfig.batchSize)
          }
            .onFailure {
              log.error("Exception fetching artifacts due for check", it)
            }
            .onSuccess {
              it.forEach {
                launch {
                  try {
                    withTimeout(resourceCheckConfig.timeoutDuration.toMillis()) {
                      artifactHandlers.applyAll(it)
                    }
                  } catch (e: TimeoutCancellationException) {
                    log.error("Timed out checking artifact $it from ${it.deliveryConfigName}", e)
                    spectator.counter(
                      "keel.scheduled.timeout",
                      listOf(BasicTag("type",  "artifactcheck"))
                    ).safeIncrement()
                  } catch (e: Exception) {
                    log.error("Failure checking artifact $it from ${it.deliveryConfigName}", e)
                    spectator.counter(
                      "keel.scheduled.failure",
                      listOf(BasicTag("type",  "artifactcheck"))
                    ).safeIncrement()
                  }
                }
              }
              spectator.counter(
                "keel.scheduled.batch.size",
                listOf(BasicTag("type", "artifactcheck"))
              ).increment(it.size.toLong())
            }
        }
      }
      runBlocking { job.join() }
      recordDuration(startTime, "artifactcheck")
    }
  }

  @Scheduled(fixedDelayString = "\${keel.environment-verification.frequency:PT1S}")
  fun verifyEnvironments() {
    if (enabled.get()) {
      val startTime = clock.instant()

      val job = launch(blankMDC) {
        supervisorScope {
          runCatching {
            repository
              .nextEnvironmentsForVerification(verificationConfig.minAgeDuration, verificationConfig.batchSize)
          }
            .onFailure {
              log.error("Exception fetching verifications due for check", it)
            }
            .onSuccess {
              it.forEach {
                launch {
                  try {
                    withTimeout(verificationConfig.timeoutDuration.toMillis()) {
                        try {
                          verificationRunner.runFor(it)
                        } catch (e: EnvironmentCurrentlyBeingActedOn) {
                          log.info("Couldn't verify ${it.version} in ${it.deliveryConfig.application}/${it.environmentName} because environment is currently being acted on", e.message)
                        }
                    }
                  } catch (e: TimeoutCancellationException) {
                    log.error("Timed out verifying ${it.version} in ${it.deliveryConfig.application}/${it.environmentName}", e)
                    spectator.counter(
                      "keel.scheduled.timeout",
                      listOf(BasicTag("type",  "verification"))
                    ).safeIncrement()
                  } catch (e: Exception) {
                    log.error("Failed verifying ${it.version} in ${it.deliveryConfig.application}/${it.environmentName}", e)
                    spectator.counter(
                      "keel.scheduled.failure",
                      listOf(BasicTag("type",  "verification"))
                    ).safeIncrement()
                  }
                }
              }
              spectator.counter(
                "keel.scheduled.batch.size",
                listOf(BasicTag("type", "verification"))
              ).increment(it.size.toLong())
            }
        }
      }

      runBlocking { job.join() }
      recordDuration(startTime, "verification")
    }
  }

  @Scheduled(fixedDelayString = "\${keel.environment-post-deploy.frequency:PT1S}")
  fun runPostDeployActions() {
    if (enabled.get()) {
      val startTime = clock.instant()

      val job = launch(blankMDC) {
        supervisorScope {
          runCatching {
            repository
              .nextEnvironmentsForPostDeployAction(postDeployConfig.minAgeDuration, postDeployConfig.batchSize)
          }
            .onFailure {
              log.error("Exception fetching post deploy actions due for check", it)
            }
            .onSuccess {
              it.forEach {
                launch {
                  try {
                    withTimeout(postDeployConfig.timeoutDuration.toMillis()) {
                      postDeployActionRunner.runFor(it)
                    }
                  } catch (e: TimeoutCancellationException) {
                    log.error("Timed out running post deploy actions on ${it.version} in ${it.deliveryConfig.application}/${it.environmentName}", e)
                    spectator.counter(
                      "keel.scheduled.timeout",
                      listOf(BasicTag("type",  "postdeploy"))
                    ).safeIncrement()
                  } catch (e: Exception) {
                    log.error("Failure running post deploy actions on ${it.version} in ${it.deliveryConfig.application}/${it.environmentName}", e)
                    spectator.counter(
                      "keel.scheduled.failure",
                      listOf(BasicTag("type",  "postdeploy"))
                    ).safeIncrement()
                  }
                }
              }
              spectator.counter(
                "keel.scheduled.batch.size",
                listOf(BasicTag("type", "postdeploy"))
              ).increment(it.size.toLong())
            }
        }
      }

      runBlocking { job.join() }
      recordDuration(startTime, "postdeploy")
    }
  }

  @Scheduled(fixedDelayString = "\${keel.task-check.frequency:PT1S}")
  fun checkTasks() {
    if (enabled.get()) {
      val startTime = clock.instant()
      val job = launch(blankMDC) {
        supervisorScope {
          runCatching {
            taskTrackingRepository.getIncompleteTasks(taskCheckConfig.minAgeDuration, taskCheckConfig.batchSize)
          }
            .onFailure {
              log.error("Exception fetching tasks due for check", it)
            }
            .onSuccess {
              it.forEach {
                launch {
                  try {
                    withTimeout(taskCheckConfig.timeoutDuration.toMillis()) {
                      taskActuator.checkTask(it)
                    }
                  } catch (e: TimeoutCancellationException) {
                    log.error("Timed out checking task ${it.id}", e)
                    spectator.counter(
                      "keel.scheduled.timeout",
                      listOf(
                        BasicTag("type",  "task")
                      )
                    ).safeIncrement()
                  } catch (e: Exception) {
                    log.error("Failed checking task ${it.id}", e)
                    spectator.counter(
                      "keel.scheduled.failure",
                      listOf(
                        BasicTag("type",  "task")
                      )
                    ).safeIncrement()
                  }
                }
              }
              spectator.counter(
                "keel.scheduled.batch.size",
                listOf(BasicTag("type", "task"))
              ).increment(it.size.toLong())
            }
        }
      }
      runBlocking { job.join() }
      recordDuration(startTime, "task")
    }
  }

  @Scheduled(fixedDelayString = "\${keel.artifact-version-cleanup.frequency:PT1H}")
  fun artifactVersionCleanup() {
    if (enabled.get()) {
      val startTime = clock.instant()
      val job = launch(blankMDC) {
        supervisorScope {
          runCatching {
            repository
              .artifactVersionCleanup(artifactVersionCleanupConfig.threshold)
          }
            .onFailure {
              log.error("Exception cleaning old artifact versions", it)
              spectator.counter(
                "keel.scheduled.failure",
                listOf(BasicTag("type",  "artifactversioncleanup"))
              ).safeIncrement()
            }
            .onSuccess {
              log.info("Successfully pruned old artifact versions")
              spectator.counter(
                "keel.scheduled.batch.size",
                listOf(BasicTag("type", "artifactversioncleanup"))
              ).safeIncrement()
            }
        }
      }
      runBlocking { job.join() }
      recordDuration(startTime, "artifactversioncleanup")
    }
  }

  private fun recordDuration(startTime: Instant, type: String) =
    spectator.recordDurationPercentile("keel.scheduled.method.duration", clock, startTime, setOf(BasicTag("type", type)))

  private val log by lazy { LoggerFactory.getLogger(javaClass) }
}
