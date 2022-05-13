package com.netflix.spinnaker.keel.scheduling

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

  override fun schedule(request: ResourceScheduler.ScheduleResourceRequest) {
    var interval = configActivites.getResourceKindCheckInterval(CheckResourceKindRequest(request.resourceKind))

    val minInterval = Duration.ofSeconds(30)
    if (minInterval > interval) {
      Workflow.getLogger(javaClass).warn(
        "Configured interval for ${request.resourceKind} ($interval) is less than minimum $minInterval: Using minimum"
      )
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
        Workflow.continueAsNew(request)
      }
    }
  }

  override fun checkNow() {
    checkNow = true
  }
}
