package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.keel.api.DeployHealth.AUTO
import com.netflix.spinnaker.keel.api.schema.Discriminator
import java.time.Duration
import java.time.Duration.ZERO

abstract class ClusterDeployStrategy {
  @Discriminator abstract val strategy: String
  abstract val health: DeployHealth

  companion object {
    val DEFAULT_WAIT_FOR_INSTANCES_UP: Duration = Duration.ofMinutes(30)
    const val RED_BLACK_STRATEGY = "red-black"
    const val HIGHLANDER_STRATEGY = "highlander"
    const val NONE_STRATEGY = "none"
    const val ROLLING_PUSH_STRATEGY = "rolling-push"
  }
}

data class RedBlack(
  override val health: DeployHealth = AUTO,
  val resizePreviousToZero: Boolean? = false,
  val maxServerGroups: Int? = 2,
  val delayBeforeDisable: Duration? = ZERO,
  val delayBeforeScaleDown: Duration? = ZERO,
  val waitForInstancesUp: Duration? = DEFAULT_WAIT_FOR_INSTANCES_UP,
) : ClusterDeployStrategy() {
  override val strategy = RED_BLACK_STRATEGY
}

data class Highlander(
  override val health: DeployHealth = AUTO
) : ClusterDeployStrategy() {
  override val strategy = HIGHLANDER_STRATEGY
}

data class NoStrategy(
  override val health: DeployHealth = AUTO
): ClusterDeployStrategy() {
  override val strategy = NONE_STRATEGY
}

data class RollingPush(
  override val health: DeployHealth = AUTO,
  val relaunchAllInstances: Boolean = true,
  /** Number of instances to terminate and relaunch at the same time */
  val numConcurrentRelaunches: Int? = null,
  val totalRelaunches: Int? = null,
  val terminationOrder: TerminationOrder? = null
): ClusterDeployStrategy() {
  override val strategy = ROLLING_PUSH_STRATEGY
}

enum class TerminationOrder {
  /** Terminates the newest instances first */
  NEWEST_FIRST,
  /** Terminates the oldest instances first */
  OLDEST_FIRST
}

enum class DeployHealth {
  /** Use Orca's default (Discovery and ELB/Target group if attached). */
  AUTO,
  /** Use only cloud provider health. */
  NONE
}

fun ClusterDeployStrategy.withDefaultsOmitted(): ClusterDeployStrategy =
  when (this) {
    is RedBlack -> {
      val defaults = RedBlack()
      RedBlack(
        maxServerGroups = nullIfDefault(maxServerGroups, defaults.maxServerGroups),
        delayBeforeDisable = nullIfDefault(delayBeforeDisable, defaults.delayBeforeDisable),
        delayBeforeScaleDown = nullIfDefault(delayBeforeScaleDown, defaults.delayBeforeScaleDown),
        resizePreviousToZero = nullIfDefault(resizePreviousToZero, defaults.resizePreviousToZero),
      )
    }
    else -> this
  }

private fun <T> nullIfDefault(value: T, default: T): T? =
  if (value == default) null else value
