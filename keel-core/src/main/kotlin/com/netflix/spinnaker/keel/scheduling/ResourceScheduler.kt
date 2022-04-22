package com.netflix.spinnaker.keel.scheduling

import com.google.common.annotations.VisibleForTesting
import com.netflix.spinnaker.keel.scheduling.activities.ActuatorActivities
import com.netflix.spinnaker.keel.scheduling.activities.ActuatorActivities.CheckResourceRequest
import com.netflix.spinnaker.keel.scheduling.activities.SchedulingConfigActivities
import com.netflix.spinnaker.keel.scheduling.activities.SchedulingConfigActivities.CheckResourceKindRequest
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import java.time.Duration
import java.time.Instant

@WorkflowInterface
interface ResourceScheduler {
  @WorkflowMethod
  fun schedule(request: ScheduleResourceRequest)

  @SignalMethod
  fun checkNow()

  data class ScheduleResourceRequest(
    val resourceId: String,
    val resourceKind: String
  )
}

class ResourceSchedulerImpl : ResourceScheduler {

  private val configActivites = SchedulingConfigActivities.get()
  private val actuatorActivities = ActuatorActivities.get()

  /**
   * When set to true, the scheduler will short-circuit its timer to initiate a check immediately.
   * Once checked, this value is set back to its default.
   */
  private var checkNow = false

  /**
   * Plumbing: Internal iterator for triggering [Workflow.continueAsNew].
   *
   * @see shouldContinueAsNew
   */
  private var iteration = 0

  /**
   * Plumbing: The maximum number of check iterations before calling [Workflow.continueAsNew].
   *
   * The iterator must be kept to limit the size of the workflow's event history to a manageable size. This estimates
   * the number of events executed per-iteration, and estimates on the side of caution rather than right-sizing.
   *
   * @see shouldContinueAsNew
   */
  @VisibleForTesting
  internal var maxIterations: Int = 1_000

  /**
   * Plumbing: Used to trigger a continue-as-new after a certain time, which will help gradual rollout of workflow
   * logic changes in long-running workflows.
   *
   * @see shouldContinueAsNew
   */
  private var startTime = Instant.ofEpochMilli(Workflow.currentTimeMillis())

  /**
   * The time when the resource was previously checked, used for emitting metrics about the duration between checks
   */
  private var lastCheckedTime: Instant? = null

  override fun schedule(request: ResourceScheduler.ScheduleResourceRequest) {
    while (true) {
      actuatorActivities.checkResource(CheckResourceRequest(request.resourceId))

      val now = Instant.ofEpochMilli(Workflow.currentTimeMillis())
      if (lastCheckedTime != null) {
        Workflow
          .getMetricsScope()
          .tagged(
            mutableMapOf(
              "type" to "resource",
              "scheduler" to "temporal"
            )
          )
          .timer("keel.periodically.checked.age")
          .record(com.uber.m3.util.Duration.ofMillis(Duration.between(lastCheckedTime, now).toMillis().toDouble()))
      }
      lastCheckedTime = now

      val interval = configActivites.getResourceKindCheckInterval(CheckResourceKindRequest(request.resourceKind))
      Workflow.await(interval) { checkNow }
      checkNow = false

      if (shouldContinueAsNew(request.resourceKind)) {
        Workflow.continueAsNew(request)
        return
      }
    }
  }

  /**
   * TODO(rz): When history size is exposed, we should convert to using that instead of [iteration] to determine CAN.
   */
  private fun shouldContinueAsNew(resourceKind: String): Boolean {
    val continueAsNewInterval = configActivites.getContinueAsNewInterval(CheckResourceKindRequest(resourceKind))
    val canInstant = startTime.plus(continueAsNewInterval)
    return iteration++ > maxIterations || Instant.ofEpochMilli(Workflow.currentTimeMillis()).isAfter(canInstant)
  }

  override fun checkNow() {
    checkNow = true
  }
}
