package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.scheduling.ResourceScheduler.ScheduleResourceRequest
import com.netflix.spinnaker.keel.scheduling.activities.ActuatorActivities
import com.netflix.spinnaker.keel.scheduling.activities.ActuatorActivities.CheckResourceRequest
import com.netflix.spinnaker.keel.scheduling.activities.SchedulingConfigActivities.CheckResourceKindRequest
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import java.time.Duration

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

class ResourceSchedulerImpl : AbstractScheduler<ScheduleResourceRequest>(Duration.ofMinutes(5)), ResourceScheduler {

  override fun getCheckInterval(request: ScheduleResourceRequest): Duration =
    configActivites.getResourceKindCheckInterval(CheckResourceKindRequest(request.resourceKind))

  override fun getScheduledType(request: ScheduleResourceRequest): String =
    request.resourceKind

  override fun getScheduledName(request: ScheduleResourceRequest): String =
    "${request.resourceKind}/${request.resourceId}"

  override fun monitorActivity(interval: Duration, request: ScheduleResourceRequest) {
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

    pollerActivity.monitorResource(
      ActuatorActivities.MonitorResourceRequest(
        request.resourceId,
        request.resourceKind
      )
    )
  }

  override fun checkNowActivity(request: ScheduleResourceRequest) {
    actuatorActivities.checkResource(CheckResourceRequest(request.resourceId))
  }
}
