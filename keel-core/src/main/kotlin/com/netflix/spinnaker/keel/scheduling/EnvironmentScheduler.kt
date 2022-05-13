package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.scheduling.EnvironmentScheduler.ScheduleEnvironmentRequest
import com.netflix.spinnaker.keel.scheduling.activities.ActuatorActivities
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import java.time.Duration

@WorkflowInterface
interface EnvironmentScheduler {

  @WorkflowMethod
  fun schedule(request: ScheduleEnvironmentRequest)

  @SignalMethod
  fun checkNow()

  data class ScheduleEnvironmentRequest(
    val application: String,
    val environment: String
  )
}

class EnvironmentSchedulerImpl : AbstractScheduler<ScheduleEnvironmentRequest>(Duration.ofMinutes(5)), EnvironmentScheduler {
  override fun getCheckInterval(request: ScheduleEnvironmentRequest): Duration =
    configActivites.getEnvironmentCheckInterval()

  override fun getScheduledType(request: ScheduleEnvironmentRequest): String =
    request.environment

  override fun getScheduledName(request: ScheduleEnvironmentRequest): String =
    "environment:${request.application}:${request.environment}"

  override fun monitorActivity(interval: Duration, request: ScheduleEnvironmentRequest) {
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

    pollerActivity.monitorEnvironment(
      ActuatorActivities.MonitorEnvironmentRequest(
        application = request.application,
        environment = request.environment
      )
    )
  }

  override fun checkNowActivity(request: ScheduleEnvironmentRequest) {
    actuatorActivities.checkEnvironment(ActuatorActivities.CheckEnvironmentRequest(
      application = request.application,
      environment = request.environment
    ))
  }
}
