package com.netflix.spinnaker.keel.scheduling.activities

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.workflow.Workflow
import java.time.Duration
import java.time.Instant

@ActivityInterface(namePrefix = "ActuatorActivities-")
interface ActuatorActivities {

  fun checkResource(request: CheckResourceRequest)

  fun monitorResource(request: MonitorResourceRequest)

  data class CheckResourceRequest(
    val resourceId: String,
    val lastChecked: Instant? = null
  )

  data class MonitorResourceRequest(
    val resourceId: String,
    val resourceKind: String,
    val lastChecked: Instant = Instant.now()
  ) {
    fun toCheckResourceRequest(lastChecked: Instant): CheckResourceRequest =
      CheckResourceRequest(resourceId, lastChecked)
  }

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
