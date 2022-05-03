package com.netflix.spinnaker.keel.scheduling

import com.google.common.annotations.VisibleForTesting
import com.netflix.spinnaker.keel.scheduling.activities.ActuatorActivities
import com.netflix.spinnaker.keel.scheduling.activities.ActuatorActivities.CheckResourceRequest
import com.netflix.spinnaker.keel.scheduling.activities.SchedulingConfigActivities
import com.netflix.spinnaker.keel.scheduling.activities.SchedulingConfigActivities.CheckResourceKindRequest
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.failure.ActivityFailure
import io.temporal.failure.CanceledFailure
import io.temporal.workflow.Async
import io.temporal.workflow.Promise
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import kotlin.math.min

@WorkflowInterface
interface ResourceScheduler {
  @WorkflowMethod
  fun schedule(request: ScheduleResourceRequest)

  @SignalMethod
  fun checkNow()

  /**
   * @param lastChecked is for internal-use only: Do not set it when initiating a new scheduler.
   */
  data class ScheduleResourceRequest(
    val resourceId: String,
    val resourceKind: String,
    val lastChecked: Instant = Instant.now()
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
    // Just splitting v1 code out into a different method so it's a little easier to code review.
    if (Workflow.getVersion("retrying-activity", Workflow.DEFAULT_VERSION, 1) == Workflow.DEFAULT_VERSION) {
    v1(request)
    } else {
      v2(request)
    }
  }

  private fun v1(request: ResourceScheduler.ScheduleResourceRequest) {
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

  private fun v2(request: ResourceScheduler.ScheduleResourceRequest) {
    var interval = configActivites.getResourceKindCheckInterval(CheckResourceKindRequest(request.resourceKind))

    // TODO(rz): 60s is advised as the lowest we should go for serverside retries. Set this to 30s because
    //  the MD team thinks 60s is too slow, but it'll likely need to switch back to 60s.
    val minInterval = Duration.ofSeconds(30)
    if (minInterval > interval) {
      Workflow.getLogger(javaClass).warn("Configured interval for ${request.resourceKind} ($interval) is less than minimum $minInterval: Using minimum")
      interval = minInterval
    }

    val pollerActivity = Workflow.newActivityStub(
      ActuatorActivities::class.java,
      ActivityOptions.newBuilder()
        .setTaskQueue(Workflow.getInfo().taskQueue)
        .setStartToCloseTimeout(Duration.ofMinutes(5))
        .setRetryOptions(
          RetryOptions.newBuilder()
            .setBackoffCoefficient(1.0)
            .setInitialInterval(interval)
            .setMaximumInterval(interval)
            .build()
        )
        .build()
    )

    // Start the resource monitor in a cancellation scope. This will allow us to efficiently poll for reconciliation
    // checks on a normal schedule while also supporting checkNow. When checkNow is signaled, the workflow will stop
    // waiting, cancel the montiorResource activity, and immediately trigger a short-lived checkResource call. Once
    // the checkResource is done, we'll CAN to reset state and start all over again.
    lateinit var promise: Promise<Void>
    val scope = Workflow.newCancellationScope(
      Runnable {
        promise = Async.procedure {
          pollerActivity.monitorResource(
            ActuatorActivities.MonitorResourceRequest(
              request.resourceId,
              request.resourceKind
            )
          )
        }
      }
    )
    scope.run()

    Workflow.await { checkNow }
    scope.cancel()

    try {
      promise.get()
    } catch (e: ActivityFailure) {
      if (e.cause is CanceledFailure && checkNow) {
        Workflow.getLogger(javaClass.simpleName).info(
          "Received checkNow signal for ${request.resourceKind}/${request.resourceId}: " +
            "Aborted poller and will continueAsNew after immediate check"
        )
        actuatorActivities.checkResource(CheckResourceRequest(request.resourceId))
        Workflow.continueAsNew(request.copy(lastChecked = Instant.ofEpochMilli(Workflow.currentTimeMillis())))
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
