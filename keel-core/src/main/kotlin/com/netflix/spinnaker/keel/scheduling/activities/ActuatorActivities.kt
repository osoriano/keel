package com.netflix.spinnaker.keel.scheduling.activities

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.workflow.Workflow
import java.time.Duration

@ActivityInterface(namePrefix = "ActuatorActivities-")
interface ActuatorActivities {

  fun checkResource(request: CheckResourceRequest)

  fun monitorResource(request: MonitorResourceRequest)

  fun checkEnvironment(request: CheckEnvironmentRequest)

  fun monitorEnvironment(request: MonitorEnvironmentRequest)

  data class CheckResourceRequest(
    val resourceId: String
  )

  data class MonitorResourceRequest(
    val resourceId: String,
    val resourceKind: String
  ) {
    fun toCheckResourceRequest(): CheckResourceRequest =
      CheckResourceRequest(resourceId)
  }

  data class CheckEnvironmentRequest(
    val application: String,
    val environment: String
  )

  data class MonitorEnvironmentRequest(
    val application: String,
    val environment: String
  ) {
    fun toCheckEnvironmentRequest(): CheckEnvironmentRequest =
      CheckEnvironmentRequest(application, environment)
  }

  companion object {
    fun get(startToClose: Duration): ActuatorActivities =
      Workflow.newActivityStub(
        ActuatorActivities::class.java,
        ActivityOptions.newBuilder()
          .setTaskQueue(Workflow.getInfo().taskQueue)
          .setStartToCloseTimeout(startToClose)
          .setRetryOptions(
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
