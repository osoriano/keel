package com.netflix.spinnaker.keel.telemetry

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.ThreadPoolMonitor
import com.netflix.spinnaker.keel.activation.ApplicationDown
import com.netflix.spinnaker.keel.activation.ApplicationUp
import com.netflix.spinnaker.keel.events.ResourceActuationLaunched
import com.netflix.spinnaker.keel.events.ResourceCheckResult
import com.netflix.spinnaker.keel.events.VerificationBlockedActuation
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Component
class TelemetryListener(
  private val spectator: Registry,
  private val clock: Clock,
  threadPoolTaskSchedulers: List<ThreadPoolTaskScheduler>,
  threadPoolTaskExecutors: List<ThreadPoolTaskExecutor>,
) {
  private val enabled = AtomicBoolean(false)

  init {
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

  @EventListener(ApplicationUp::class)
  fun onApplicationUp() {
    enabled.set(true)
  }

  @EventListener(ApplicationDown::class)
  fun onApplicationDown() {
    enabled.set(false)
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

  @EventListener(ArtifactVersionVetoed::class)
  fun onArtifactVersionVetoed(event: ArtifactVersionVetoed) {
    spectator.counter(
      ARTIFACT_VERSION_VETOED,
      listOf(BasicTag("application", event.application))
    )
      .safeIncrement()
  }

  @EventListener(EnvironmentCheckComplete::class)
  fun onEnvironmentCheckComplete(event: EnvironmentCheckComplete) {
    spectator.timer(
      ENVIRONMENT_CHECK_DURATION_ID,
      listOf(BasicTag("application", event.application))
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

  @EventListener(VerificationBlockedActuation::class)
  fun onBlockedActuation(event: VerificationBlockedActuation) {
    spectator.counter(BLOCKED_ACTUATION_ID,
      listOf(
        BasicTag("resourceId", event.id),
        BasicTag("resourceKind", event.kind.toString()),
        BasicTag("resourceApplication", event.application)
      )
    ).safeIncrement()
  }

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  companion object {
    private const val TIME_SINCE_LAST_CHECK = "keel.periodically.checked.age"
    private const val RESOURCE_CHECKED_COUNTER_ID = "keel.resource.checked"
    private const val RESOURCE_CHECK_SKIPPED_COUNTER_ID = "keel.resource.check.skipped"
    private const val RESOURCE_ACTUATION_LAUNCHED_COUNTER_ID = "keel.resource.actuation.launched"
    private const val ARTIFACT_APPROVED_COUNTER_ID = "keel.artifact.approved"
    private const val ENVIRONMENT_CHECK_DURATION_ID = "keel.environment.check.duration"
    private const val ARTIFACT_VERSION_VETOED = "keel.artifact.version.vetoed"
    private const val VERIFICATION_COMPLETED_COUNTER_ID = "keel.verification.completed"
    private const val VERIFICATION_STARTED_COUNTER_ID = "keel.verification.started"
    private const val INVALID_VERIFICATION_ID_SEEN_COUNTER_ID = "keel.verification.invalid.id.seen"
    private const val BLOCKED_ACTUATION_ID = "keel.actuation.blocked"
    private const val AGENT_DURATION_ID = "keel.agent.duration"
  }
}
