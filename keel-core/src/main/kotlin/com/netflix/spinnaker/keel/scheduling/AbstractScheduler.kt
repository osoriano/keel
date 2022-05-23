package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.scheduling.activities.ActuatorActivities
import com.netflix.spinnaker.keel.scheduling.activities.SchedulingConfigActivities
import io.temporal.failure.ActivityFailure
import io.temporal.failure.CanceledFailure
import io.temporal.workflow.Async
import io.temporal.workflow.ContinueAsNewOptions
import io.temporal.workflow.Promise
import io.temporal.workflow.Workflow
import java.time.Duration

abstract class AbstractScheduler<T>(
  checkStartToCloseTimeout: Duration
) {

  protected val configActivites = SchedulingConfigActivities.get()
  protected val actuatorActivities = ActuatorActivities.get(checkStartToCloseTimeout)

  /**
   * When set to true, the scheduler will short-circuit its timer to initiate a check immediately.
   * Once checked, this value is set back to its default.
   */
  private var checkNow = false

  /**
   * Retrieve the configured check interval for the given scheduled request.
   */
  abstract fun getCheckInterval(request: T): Duration

  /**
   * Returns the overall type of scheduled item. Used for logging.
   */
  abstract fun getScheduledType(request: T): String

  /**
   * Returns the specific name of the scheduled item. Used for logging.
   */
  abstract fun getScheduledName(request: T): String

  /**
   * Callback for performing the long-polling monitor activity.
   *
   * This method must be blocking, meaning, the activity must never complete successfully.
   */
  abstract fun monitorActivity(interval: Duration, request: T)

  /**
   * Perform the check-now activity. This activity must not block.
   */
  abstract fun checkNowActivity(request: T)

  fun schedule(request: T) {
    var interval = getCheckInterval(request)

    val minInterval = Duration.ofSeconds(30)
    if (minInterval > interval) {
      Workflow.getLogger(javaClass).warn(
        "Configured interval for ${getScheduledType(request)} ($interval) is less than minimum $minInterval: Using minimum"
      )
      interval = minInterval
    }

    // Start the resource/environment monitor in a cancellation scope. This will allow us to efficiently poll for reconciliation
    // checks on a normal schedule while also supporting checkNow. When checkNow is signaled, the workflow will stop
    // waiting, cancel the monitorResource activity, and immediately trigger a short-lived checkNowActivity call. Once
    // the checkNowActivity is done, we'll CAN to reset state and start all over again.
    lateinit var promise: Promise<Void>
    val scope = Workflow.newCancellationScope(
      Runnable {
        promise = Async.procedure {
          monitorActivity(interval, request)
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
          "Received checkNow signal for ${getScheduledName(request)}: " +
            "Aborted poller and will continueAsNew after immediate check"
        )
        checkNowActivity(request)
        // TODO(rz): CAN does not carry-forward search attributes by default.
        //  https://github.com/temporalio/sdk-java/issues/1200
        Workflow.continueAsNew(
          ContinueAsNewOptions.newBuilder()
            .setSearchAttributes(
              mapOf(
                SchedulingConsts.WORKER_ENV_SEARCH_ATTRIBUTE to Workflow.getSearchAttribute<String>(SchedulingConsts.WORKER_ENV_SEARCH_ATTRIBUTE)
              )
            )
            .build(),
          request
        )
      }
    }
  }

  fun checkNow() {
    checkNow = true
  }
}
