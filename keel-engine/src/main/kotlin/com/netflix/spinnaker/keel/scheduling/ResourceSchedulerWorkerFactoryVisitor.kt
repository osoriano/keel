package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.scheduling.activities.ActuatorActivities
import com.netflix.spinnaker.keel.scheduling.activities.SchedulingConfigActivities
import com.netflix.temporal.core.WorkerFactoryVisitor
import com.netflix.temporal.core.convention.TaskQueueNamer
import io.temporal.worker.WorkerFactory
import io.temporal.worker.WorkerOptions
import org.springframework.stereotype.Component

@Component
class ResourceSchedulerWorkerFactoryVisitor(
  private val schedulingConfigActivities: SchedulingConfigActivities,
  private val actuatorActivities: ActuatorActivities
) : WorkerFactoryVisitor {
  override val name: String = "resource-scheduler"

  override fun accept(workerFactory: WorkerFactory, workerOptions: WorkerOptions, taskQueueNamer: TaskQueueNamer) {
    workerFactory.newWorker(taskQueueNamer.name(RESOURCE_SCHEDULER_TASK_QUEUE)).apply {
      registerWorkflowImplementationTypes(ResourceSchedulerImpl::class.java)
      registerActivitiesImplementations(schedulingConfigActivities, actuatorActivities, taskQueueNamer)
    }
  }
}
