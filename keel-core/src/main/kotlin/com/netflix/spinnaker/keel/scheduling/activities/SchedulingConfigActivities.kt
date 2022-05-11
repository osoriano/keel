package com.netflix.spinnaker.keel.scheduling.activities

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.workflow.Workflow
import java.time.Duration

@ActivityInterface(namePrefix = "SchedulingConfigActivities-")
interface SchedulingConfigActivities {

  fun getResourceKindCheckInterval(request: CheckResourceKindRequest): Duration

  fun getContinueAsNewInterval(request: CheckResourceKindRequest): Duration

  fun getEnvironmentCheckInterval(): Duration

  data class CheckResourceKindRequest(
    val resourceKind: String
  )

  companion object {
    fun get(): SchedulingConfigActivities =
      Workflow.newActivityStub(
        SchedulingConfigActivities::class.java,
        ActivityOptions.newBuilder()
          .setTaskQueue(Workflow.getInfo().taskQueue)
          .setScheduleToCloseTimeout(Duration.ofSeconds(10))
          .setStartToCloseTimeout(Duration.ofSeconds(10))
          .setRetryOptions(
            RetryOptions.newBuilder()
              .setBackoffCoefficient(1.1)
              .setInitialInterval(Duration.ofSeconds(2))
              .setMaximumInterval(Duration.ofSeconds(30))
              .build()
          )
          .build()
      )
  }
}
