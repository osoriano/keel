package com.netflix.spinnaker.keel.telemetry

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter
import com.netflix.spectator.api.patterns.ThreadPoolMonitor
import com.netflix.spinnaker.keel.activation.DiscoveryActivated
import com.netflix.spinnaker.keel.actuation.ScheduledArtifactCheckStarting
import com.netflix.spinnaker.keel.actuation.ScheduledEnvironmentCheckStarting
import com.netflix.spinnaker.keel.actuation.ScheduledEnvironmentVerificationStarting
import com.netflix.spinnaker.keel.actuation.ScheduledPostDeployActionRunStarting
import com.netflix.spinnaker.keel.api.ResourceStatus.HAPPY
import com.netflix.spinnaker.keel.api.events.ArtifactVersionMarkedDeploying
import com.netflix.spinnaker.keel.api.events.ArtifactVersionStored
import com.netflix.spinnaker.keel.events.ArtifactDeployedNotification
import com.netflix.spinnaker.keel.events.ResourceActuationLaunched
import com.netflix.spinnaker.keel.events.ResourceCheckResult
import com.netflix.spinnaker.keel.events.VerificationBlockedActuation
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.rollout.FeatureRolloutAttempted
import com.netflix.spinnaker.keel.rollout.FeatureRolloutFailed
import org.springframework.context.event.EventListener
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

@Component
class TelemetryListener(
  private val spectator: Registry,
  private val clock: Clock,
  private val repository: KeelRepository,
  threadPoolTaskSchedulers: List<ThreadPoolTaskScheduler>,
  threadPoolTaskExecutors: List<ThreadPoolTaskExecutor>,
): DiscoveryActivated() {
  private val lastResourceCheck: AtomicReference<Instant> =
    createDriftGauge(RESOURCE_CHECK_DRIFT_GAUGE)
  private val lastEnvironmentCheck: AtomicReference<Instant> =
    createDriftGauge(ENVIRONMENT_CHECK_DRIFT_GAUGE)
  private val lastArtifactCheck: AtomicReference<Instant> =
    createDriftGauge(ARTIFACT_CHECK_DRIFT_GAUGE)
  private val lastVerificationCheck: AtomicReference<Instant> =
    createDriftGauge(VERIFICATION_CHECK_DRIFT_GAUGE)
  private val lastPostDeployCheck: AtomicReference<Instant> =
    createDriftGauge(POST_DEPLOY_CHECK_DRIFT_GAUGE)

  init {
    // monitor the overall count of delivery configs in the system
    PolledMeter
      .using(spectator)
      .withName(DELIVERY_CONFIG_COUNTER_ID)
      .monitorValue(repository) {
        repository.getDeliveryConfigCount().toDouble()
      }

    // monitor the overall count of environments in the system
    PolledMeter
      .using(spectator)
      .withName(ENVIRONMENT_COUNTER_ID)
      .monitorValue(repository) {
        repository.getEnvironmentCount().toDouble()
      }

    // monitor the overall count of resources in the system
    PolledMeter
      .using(spectator)
      .withName(RESOURCE_COUNTER_ID)
      .monitorValue(repository) {
        repository.getResourceCount().toDouble()
      }

    // attach monitors for all the thread pools we have
    threadPoolTaskSchedulers.forEach { executor ->
      ThreadPoolMonitor.attach(spectator, executor.scheduledThreadPoolExecutor, executor.threadNamePrefix + "spring")
    }

    threadPoolTaskExecutors.forEach { executor ->
      ThreadPoolMonitor.attach(spectator, executor.threadPoolExecutor, executor.threadNamePrefix + "spring")
    }

    // todo: add coroutines once you can actually monitor them as described here: https://github.com/Kotlin/kotlinx.coroutines/issues/1360
    // need to monitor Dispatchers.Default, Dispatchers.IO, and Dispatchers.Unconfined
  }

  @EventListener(AboutToBeChecked::class)
  fun onAboutToBeChecked(event: AboutToBeChecked) {
    if (event.lastCheckedAt == Instant.EPOCH.plusSeconds(1)) {
      // recheck was triggered or resource is new, ignore this
      return
    }

    spectator.recordDurationPercentile(
      TIME_SINCE_LAST_CHECK,
      startTime = event.lastCheckedAt,
      endTime = clock.instant(),
      range = Duration.ofSeconds(1) to Duration.ofHours(3),
      tags = setOf(
        BasicTag("identifier", event.identifier),
        BasicTag("type", event.type)
      )
    )
  }

  @EventListener(ResourceCheckResult::class)
  fun onResourceChecked(event: ResourceCheckResult) {
    spectator.counter(
      RESOURCE_CHECKED_COUNTER_ID,
      listOf(
        BasicTag("resourceId", event.id),
        BasicTag("resourceKind", event.kind.toString()),
        BasicTag("resourceState", event.state.name),
        BasicTag("resourceApplication", event.application)
      )
    ).safeIncrement()
  }

  @EventListener(ResourceCheckSkipped::class)
  fun onResourceCheckSkipped(event: ResourceCheckSkipped) {
    spectator.counter(
      RESOURCE_CHECK_SKIPPED_COUNTER_ID,
      listOf(
        BasicTag("resourceId", event.id),
        BasicTag("resourceKind", event.kind.toString()),
        BasicTag("skipper", event.skipper)
      )
    ).safeIncrement()
  }

  @EventListener(ResourceCheckTimedOut::class)
  fun onResourceCheckTimedOut(event: ResourceCheckTimedOut) {
    spectator.counter(
      RESOURCE_CHECK_TIMED_OUT_ID,
      listOf(
        BasicTag("kind", event.kind.kind),
        BasicTag("resourceId", event.id),
        BasicTag("application", event.application)
      )
    ).safeIncrement()
  }

  @EventListener(ResourceLoadFailed::class)
  fun onResourceLoadFailed(event: ResourceLoadFailed) {
    spectator.counter(RESOURCE_LOAD_FAILED_ID).safeIncrement()
  }

  @EventListener(EnvironmentsCheckTimedOut::class)
  fun onEnvironmentsCheckTimedOut(event: EnvironmentsCheckTimedOut) {
    spectator.counter(
      ENVIRONMENT_CHECK_TIMED_OUT_ID,
      listOf(
        BasicTag("application", event.application),
        BasicTag("deliveryConfig", event.deliveryConfigName)
      )
    ).safeIncrement()
  }

  @EventListener(ArtifactVersionApproved::class)
  fun onArtifactVersionUpdated(event: ArtifactVersionApproved) {
    spectator.counter(
      ARTIFACT_APPROVED_COUNTER_ID,
      listOf(
        BasicTag("application", event.application),
        BasicTag("environment", event.environmentName),
        BasicTag("artifactName", event.artifactName),
        BasicTag("artifactType", event.artifactType)
      )
    ).safeIncrement()
  }

  @EventListener(ResourceActuationLaunched::class)
  fun onResourceActuationLaunched(event: ResourceActuationLaunched) {
    spectator.counter(
      RESOURCE_ACTUATION_LAUNCHED_COUNTER_ID,
      listOf(
        BasicTag("resourceId", event.id),
        BasicTag("resourceKind", event.kind.toString()),
        BasicTag("resourceApplication", event.application)
      )
    ).safeIncrement()
  }

  @EventListener(ResourceCheckStarted::class)
  fun onResourceCheckStarted(event: ResourceCheckStarted) {
    spectator.counter(
      RESOURCE_CHECK_STARTED_COUNTER_ID,
      listOf(
        BasicTag("resourceKind", event.resource.kind.toString())
      )
    ).safeIncrement()
  }

  @EventListener(ResourceCheckCompleted::class)
  fun onResourceCheckCompleted(event: ResourceCheckCompleted) {
    lastResourceCheck.set(clock.instant())
  }

  @EventListener(EnvironmentCheckStarted::class)
  fun onEnvironmentCheckStarted(event: EnvironmentCheckStarted) {
    spectator.counter(
      ENVIRONMENT_CHECK_STARTED_COUNTER_ID,
      listOf(
        BasicTag("application", event.deliveryConfig.application)
      )
    ).safeIncrement()
  }

  @EventListener(ResourceCheckCompleted::class)
  fun onEnvironmentCheckComplete(event: ResourceCheckCompleted) {
    spectator.timer(
      RESOURCE_CHECK_DURATION_ID,
    ).record(event.duration)
  }

  @EventListener(ScheduledEnvironmentCheckStarting::class)
  fun onScheduledEnvironmentCheckStarting(event: ScheduledEnvironmentCheckStarting) {
    lastEnvironmentCheck.set(clock.instant())
  }

  @EventListener(ScheduledArtifactCheckStarting::class)
  fun onScheduledArtifactCheckStarting(event: ScheduledArtifactCheckStarting) {
    lastArtifactCheck.set(clock.instant())
  }

  @EventListener(ScheduledEnvironmentVerificationStarting::class)
  fun onScheduledVerificationCheckStarting(event: ScheduledEnvironmentVerificationStarting) {
    lastVerificationCheck.set(clock.instant())
  }

  @EventListener(ScheduledPostDeployActionRunStarting::class)
  fun onScheduledPostDeployActionCheckStarting(event: ScheduledPostDeployActionRunStarting) {
    lastPostDeployCheck.set(clock.instant())
  }

  @EventListener(ArtifactVersionVetoed::class)
  fun onArtifactVersionVetoed(event: ArtifactVersionVetoed) {
    spectator.counter(
      ARTIFACT_VERSION_VETOED,
      listOf(BasicTag("application", event.application))
    )
      .safeIncrement()
  }

  @EventListener(ArtifactCheckComplete::class)
  fun onArtifactCheckComplete(event: ArtifactCheckComplete) {
    spectator.timer(
      ARTIFACT_CHECK_DURATION_ID,
    ).record(event.duration)
  }

  @EventListener(EnvironmentCheckComplete::class)
  fun onEnvironmentCheckComplete(event: EnvironmentCheckComplete) {
    spectator.timer(
      ENVIRONMENT_CHECK_DURATION_ID,
      listOf(BasicTag("application", event.application))
    ).record(event.duration)
  }

  @EventListener(VerificationCheckComplete::class)
  fun onVerificationCheckComplete(event: VerificationCheckComplete) {
    spectator.timer(
      VERIFICATION_CHECK_DURATION_ID,
    ).record(event.duration)
  }

  @EventListener(AgentInvocationComplete::class)
  fun onAgentInvocationComplete(event: AgentInvocationComplete) {
    spectator.timer(
      AGENT_DURATION_ID,
      listOf(BasicTag("agent", event.agentName))
    ).record(event.duration)
  }

  @EventListener(VerificationCompleted::class)
  fun onVerificationCompleted(event: VerificationCompleted) {
    spectator.counter(
      VERIFICATION_COMPLETED_COUNTER_ID,
      listOf(
        BasicTag("application", event.application),
        BasicTag("verificationType", event.verificationType),
        BasicTag("status", event.status.name)
      )
    ).safeIncrement()
  }

  @EventListener(VerificationStarted::class)
  fun onVerificationStarted(event: VerificationStarted) {
    spectator.counter(
      VERIFICATION_STARTED_COUNTER_ID,
      listOf(
        BasicTag("application", event.application),
        BasicTag("verificationType", event.verificationType)
      )
    ).safeIncrement()
  }

  @EventListener(InvalidVerificationIdSeen::class)
  fun onInvalidVerificationId(event: InvalidVerificationIdSeen) {
    spectator.counter(
      INVALID_VERIFICATION_ID_SEEN_COUNTER_ID,
      listOf(
        BasicTag("application", event.application),
        BasicTag("invalidId", event.id)
      )
    ).safeIncrement()
  }

  @EventListener(PostDeployActionCheckComplete::class)
  fun onPostDeployCheckCompleted(event: PostDeployActionCheckComplete) {
    spectator.timer(
      POST_DEPLOY_CHECK_DURATION_ID,
    ).record(event.duration)
  }

  @EventListener(VerificationBlockedActuation::class)
  fun onBlockedActuation(event: VerificationBlockedActuation) {
    spectator.counter(
      BLOCKED_ACTUATION_ID,
      listOf(
        BasicTag("resourceId", event.id),
        BasicTag("resourceKind", event.kind.toString()),
        BasicTag("resourceApplication", event.application)
      )
    ).safeIncrement()
  }

  @EventListener(FeatureRolloutAttempted::class)
  fun onFeatureRolloutAttempted(event: FeatureRolloutAttempted) {
    spectator.counter(
      FEATURE_ROLLOUT_ATTEMPTED_ID,
      listOf(
        BasicTag("feature", event.feature),
        BasicTag("resourceId", event.resourceId)
      )
    ).safeIncrement()
  }

  @EventListener(FeatureRolloutFailed::class)
  fun onFeatureRolloutFailed(event: FeatureRolloutFailed) {
    spectator.counter(
      FEATURE_ROLLOUT_FAILED_ID,
      listOf(
        BasicTag("feature", event.feature),
        BasicTag("resourceId", event.resourceId)
      )
    ).safeIncrement()
  }

  @EventListener(ArtifactVersionStored::class)
  fun onArtifactVersionStored(event: ArtifactVersionStored) {
    with(event.publishedArtifact) {
      if (createdAt != null) {
        // record how long it took us to store this version since the artifact was created
        log.debug("Recording storage delay for $type:$name: ${Duration.between(createdAt!!, clock.instant())}")
        spectator.recordDuration(ARTIFACT_DELAY, createdAt!!, clock.instant(),
          "delayType" to "storage",
          "artifactType" to type,
          "artifactName" to name
        )
      }
    }
  }

  @EventListener(ArtifactVersionMarkedDeploying::class)
  fun onArtifactVersionMarkedDeploying(event: ArtifactVersionMarkedDeploying) {
    with(event) {
      val approvedAt = repository.getApprovedAt(deliveryConfig, artifact, version, environment.name)
      val pinnedAt = repository.getPinnedAt(deliveryConfig, artifact, version, environment.name)
      val startTime = when {
        pinnedAt != null && approvedAt == null -> pinnedAt
        pinnedAt == null && approvedAt != null -> approvedAt
        pinnedAt != null && approvedAt != null -> when {
          pinnedAt.isAfter(approvedAt) -> pinnedAt
          else -> approvedAt
        }
        else -> null
      }
      val action = if (startTime == pinnedAt) "pinned" else "approved"

      // record how long it took us to deploy this version since it was approved or pinned
      if (startTime != null) {
        log.debug("Recording deployment delay since $action for $artifact: ${Duration.between(startTime, timestamp)}")
        spectator.recordDuration(
          ARTIFACT_DELAY, startTime, timestamp,
          "delayType" to "deployment",
          "artifactType" to artifact.type,
          "artifactName" to artifact.name,
          "action" to action
        )
      }

      // for environments with no constraints, record how long it took us to deploy this version since it was stored
      if (environment.constraints.isEmpty()) {
        val publishedArtifact = repository.getArtifactVersion(artifact, version)
        if (publishedArtifact?.storedAt != null) {
          log.debug("Recording deployment delay since storing for $artifact: ${Duration.between(publishedArtifact.storedAt!!, timestamp)}")
          spectator.recordDuration(
            ARTIFACT_DELAY, publishedArtifact.storedAt!!, timestamp,
            "delayType" to "deployment",
            "artifactType" to artifact.type,
            "artifactName" to artifact.name,
            "action" to "stored"
          )
        }
      }

      // calculate delay from meeting the last constraint to starting the deployment
      // this will not include bake time for debians because of the implicit ImageExistsConstraint.
      val constraintStates = repository.constraintStateFor(deliveryConfig.name, environment.name, version, artifact.reference)
      val finalApprovalTime = constraintStates.mapNotNull { it.judgedAt }.maxOrNull() ?: startTime
      if (finalApprovalTime != null) {
        log.debug("Recording deployment delay since last constraint approval for $artifact: ${Duration.between(finalApprovalTime, timestamp)}")
        spectator.recordDuration(
          ARTIFACT_DELAY, finalApprovalTime, timestamp,
          "delayType" to "deployment",
          "artifactType" to artifact.type,
          "artifactName" to artifact.name,
          "action" to "constraints-met"
        )
      }
    }
  }

  @EventListener(ArtifactDeployedNotification::class)
  fun onArtifactVersionMarkedDeployed(event: ArtifactDeployedNotification) {
    with(event) {
      val resource = config.resourcesUsing(deliveryArtifact.reference, targetEnvironment.name).firstOrNull()
        ?: return

      // retrieve the resource status to check if it's happy and get the timestamp -- this costs us an extra
      // database call, but we need the metric
      val happyTime = repository.getResourceStatus(resource.id)
        ?.let {
          if (it.status == HAPPY) {
            it.updatedAt
          } else {
            null
          }
        }

      // record how long it took us to mark this version deployed since we deemed the parent resource "happy"
      if (happyTime != null) {
        log.debug("Recording delay for $deliveryArtifact: ${Duration.between(happyTime, clock.instant())}")
        spectator.recordDuration(
          ARTIFACT_DELAY, happyTime, clock.instant(),
          "delayType" to "resourceHappy",
          "artifactType" to deliveryArtifact.type,
          "artifactName" to deliveryArtifact.name
        )
      }
    }
  }

  private fun secondsSince(start: AtomicReference<Instant>): Double =
    Duration
      .between(start.get(), clock.instant())
      .toMillis()
      .toDouble()
      .div(1000)

  private fun createDriftGauge(name: String): AtomicReference<Instant> =
    PolledMeter
      .using(spectator)
      .withName(name)
      .monitorValue(AtomicReference(clock.instant())) { previous ->
        when(enabled.get()) {
          true -> secondsSince(previous)
          false -> 0.0
        }
      }

  companion object {
    private const val TIME_SINCE_LAST_CHECK = "keel.periodically.checked.age"
    private const val RESOURCE_COUNTER_ID = "keel.resource.count"
    private const val RESOURCE_CHECKED_COUNTER_ID = "keel.resource.checked"
    private const val RESOURCE_CHECK_STARTED_COUNTER_ID = "keel.resource.check.started"
    private const val RESOURCE_CHECK_SKIPPED_COUNTER_ID = "keel.resource.check.skipped"
    private const val RESOURCE_CHECK_TIMED_OUT_ID = "keel.resource.check.timeout"
    private const val RESOURCE_LOAD_FAILED_ID = "keel.resource.load.failed"
    private const val RESOURCE_ACTUATION_LAUNCHED_COUNTER_ID = "keel.resource.actuation.launched"
    private const val RESOURCE_CHECK_DURATION_ID = "keel.resource.check.duration"
    private const val ARTIFACT_CHECK_DRIFT_GAUGE = "keel.artifact.check.drift"
    private const val ARTIFACT_APPROVED_COUNTER_ID = "keel.artifact.approved"
    private const val ARTIFACT_CHECK_DURATION_ID = "keel.artifact.check.duration"
    private const val RESOURCE_CHECK_DRIFT_GAUGE = "keel.resource.check.drift"
    private const val ENVIRONMENT_COUNTER_ID = "keel.environment.count"
    private const val ENVIRONMENT_CHECK_STARTED_COUNTER_ID = "keel.environment.check.started"
    private const val ENVIRONMENT_CHECK_DRIFT_GAUGE = "keel.environment.check.drift"
    private const val ENVIRONMENT_CHECK_TIMED_OUT_ID = "keel.environment.check.timeout"
    private const val ENVIRONMENT_CHECK_DURATION_ID = "keel.environment.check.duration"
    private const val ARTIFACT_VERSION_VETOED = "keel.artifact.version.vetoed"
    private const val VERIFICATION_COMPLETED_COUNTER_ID = "keel.verification.completed"
    private const val VERIFICATION_STARTED_COUNTER_ID = "keel.verification.started"
    private const val VERIFICATION_CHECK_DRIFT_GAUGE = "keel.verification.check.drift"
    private const val VERIFICATION_CHECK_DURATION_ID = "keel.verification.check.duration"
    private const val INVALID_VERIFICATION_ID_SEEN_COUNTER_ID = "keel.verification.invalid.id.seen"
    private const val BLOCKED_ACTUATION_ID = "keel.actuation.blocked"
    private const val AGENT_DURATION_ID = "keel.agent.duration"
    private const val POST_DEPLOY_CHECK_DRIFT_GAUGE = "keel.post-deploy.check.drift"
    private const val POST_DEPLOY_CHECK_DURATION_ID = "keel.post-deploy.check.duration"
    private const val FEATURE_ROLLOUT_ATTEMPTED_ID = "keel.feature-rollout.attempted"
    private const val FEATURE_ROLLOUT_FAILED_ID = "keel.feature-rollout.failed"
    private const val DELIVERY_CONFIG_COUNTER_ID = "keel.config.count"
    const val ARTIFACT_DELAY = "artifact.delay"
  }
}
