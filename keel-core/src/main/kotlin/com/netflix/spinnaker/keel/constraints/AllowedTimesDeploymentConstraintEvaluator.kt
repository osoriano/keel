package com.netflix.spinnaker.keel.constraints

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.constraints.ConstraintRepository
import com.netflix.spinnaker.keel.api.constraints.ConstraintState
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PASS
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PENDING
import com.netflix.spinnaker.keel.api.constraints.DeploymentConstraintEvaluator
import com.netflix.spinnaker.keel.api.constraints.SupportedConstraintAttributesType
import com.netflix.spinnaker.keel.api.constraints.SupportedConstraintType
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.core.api.AllowedTimesConstraint
import com.netflix.spinnaker.keel.core.api.ZonedDateTimeRange
import com.netflix.spinnaker.keel.core.api.activeWindowOrNull
import com.netflix.spinnaker.keel.core.api.windowRange
import com.netflix.spinnaker.keel.core.api.windowsNumeric
import com.netflix.spinnaker.keel.core.api.zone
import com.netflix.spinnaker.keel.persistence.ArtifactRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.ZoneId
import org.springframework.core.env.Environment as SpringEnv

/**
 * A deployment constraint to gate promotions based on allowed time windows.
 * Multiple windows can be specified.
 *
 * Hours and days parameters support a mix of ranges and comma separated values.
 *
 * Days can be specified using full or short names according to the default JVM locale.
 *
 * A timezone can be set in the constraint and a system-wide default can by set via
 * the dynamic property "default.time-zone" which defaults to UTC.
 *
 * Example env constraint:
 *
 * ```
 * constraints:
 *  - type: allowed-times
 *    windows:
 *      - days: Monday,Tuesday-Friday
 *        hours: 11-16
 *      - days: Wed
 *        hours: 12-14
 *    tz: America/Los_Angeles
 *    maxDeploysPerWindow: 2
 * ```
 */
@JvmDefaultWithoutCompatibility  // see https://youtrack.jetbrains.com/issue/KT-39603
@Component
class AllowedTimesDeploymentConstraintEvaluator(
  override val repository: ConstraintRepository,
  override val eventPublisher: EventPublisher,
  private val artifactRepository: ArtifactRepository,
  private val environment: SpringEnv,
  override val clock: Clock
): DeploymentConstraintEvaluator<AllowedTimesConstraint, AllowedTimesConstraintAttributes>(repository, clock) {
  companion object {
    const val CONSTRAINT_NAME = "allowed-times"
  }

  override val attributeType = SupportedConstraintAttributesType<AllowedTimesConstraintAttributes>(CONSTRAINT_NAME)

  override val supportedType = SupportedConstraintType<AllowedTimesConstraint>(CONSTRAINT_NAME)

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  private fun defaultTimeZone(): ZoneId =
    ZoneId.of(
      environment.getProperty("default.time-zone", String::class.java, "America/Los_Angeles")
    )

  override suspend fun calculateConstraintState(
    artifact: DeliveryArtifact,
    version: String,
    deliveryConfig: DeliveryConfig,
    targetEnvironment: Environment,
    constraint: AllowedTimesConstraint,
    state: ConstraintState
  ): ConstraintState {
    val windowRange = currentWindowOrNull(constraint)
    val actualVsMaxDeploys = if (windowRange != null && constraint.maxDeploysPerWindow != null) {
      actualVsMaxDeploys(constraint, deliveryConfig, artifact, targetEnvironment, windowRange)
    } else {
      null
    }
    val actualDeploys = actualVsMaxDeploys?.first
    val maxDeploys = actualVsMaxDeploys?.second

    val newState = when {
      windowRange == null -> state.copy(status = PENDING, comment = "Not currently in an allowed time window, blocking deployment.")
      actualVsMaxDeploys == null -> state.copy(status = PASS, comment = null)
      canDeploy(actualDeploys, maxDeploys) -> {
        log.debug(
          "{}:{} has only been deployed {} times during the current window ({} to {}), allowing deployment",
          deliveryConfig.name,
          targetEnvironment.name,
          actualVsMaxDeploys.first,
          windowRange.start.toLocalTime(),
          windowRange.endInclusive.toLocalTime()
        )
        state.copy(status = PASS, comment = "Allowing deployment #${(actualDeploys ?: 0) + 1} in window")
      }
      else -> {
        log.info(
          "{}:{} has already been deployed {} times during the current window ({} to {}), skipping deployment",
          deliveryConfig.name,
          targetEnvironment.name,
          actualVsMaxDeploys.first,
          windowRange.start.toLocalTime(),
          windowRange.endInclusive.toLocalTime()
        )
        state.copy(status = PENDING, comment = "Max number of deploys reached within current time window (${actualVsMaxDeploys.second}). Blocking deployment until the next window.")
      }
    }

    return newState.copy(
      attributes = AllowedTimesConstraintAttributes(
        allowedTimes = constraint.windowsNumeric,
        timezone = constraint.tz ?: defaultTimeZone().id,
        actualDeploys = actualVsMaxDeploys?.first,
        maxDeploys = actualVsMaxDeploys?.second,
        currentlyPassing = newState.status.passes()
      ),
      judgedAt = clock.instant()
    )
  }

  fun canDeploy(actualDeploys: Int?, maxDeploys: Int?): Boolean {
    if (actualDeploys == null || maxDeploys == null) return false
    return actualDeploys < maxDeploys
  }

  private fun actualVsMaxDeploys(
    constraint: AllowedTimesConstraint,
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    targetEnvironment: Environment,
    windowRange: ZonedDateTimeRange
  ): Pair<Int, Int>? = if (constraint.maxDeploysPerWindow != null) {
    artifactRepository.versionsDeployedBetween(
      deliveryConfig,
      artifact,
      targetEnvironment.name,
      windowRange.start.toInstant(),
      windowRange.endInclusive.toInstant()
    ) to constraint.maxDeploysPerWindow
  } else {
    null
  }

  private fun currentWindowOrNull(constraint: AllowedTimesConstraint): ZonedDateTimeRange? {
    val tz = constraint.zone ?: defaultTimeZone()

    val now = clock
      .instant()
      .atZone(tz)

    return constraint.activeWindowOrNull(now)?.windowRange(now)
  }

  override fun shouldAlwaysReevaluate(): Boolean = true
}
