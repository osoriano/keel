package com.netflix.spinnaker.keel.scheduling.activities

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.workflow.Workflow
import java.time.Duration

@ActivityInterface(namePrefix = "ActuatorActivities-")
interface ActuatorActivities {

  fun checkResource(request: CheckResourceRequest)

  data class CheckResourceRequest(
    val resourceId: String
  )

  companion object {
    fun get(): ActuatorActivities =
      Workflow.newActivityStub(
        ActuatorActivities::class.java,
        ActivityOptions.newBuilder()
          .setTaskQueue(Workflow.getInfo().taskQueue)
          .setStartToCloseTimeout(Duration.ofMinutes(5))
          .setRetryOptions(
            // TODO(rz): What are the exceptions that we'll need to make non-retryable? Any?
            RetryOptions.newBuilder()
              .setBackoffCoefficient(1.1)
              .setInitialInterval(Duration.ofSeconds(2))
              .setMaximumInterval(Duration.ofMinutes(2))
              .build()
          )
          .build()
      )
  }
}
