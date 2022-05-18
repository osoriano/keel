package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.scheduling.SchedulerSupervisor.SupervisorType.ENVIRONMENT
import com.netflix.spinnaker.keel.scheduling.SchedulerSupervisor.SupervisorType.RESOURCE
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.TEMPORAL_NAMESPACE
import com.netflix.temporal.core.convention.TaskQueueNamer
import com.netflix.temporal.spring.WorkflowClientProvider
import com.netflix.temporal.spring.WorkflowServiceStubsProvider
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Clock

/**
 * Admin endpoints that deal with supervisor workflows
 */
@Service
class TemporalSupervisorAdminService(
  private val taskQueueNamer: TaskQueueNamer,
  private val workflowClientProvider: WorkflowClientProvider,
  private val workflowServiceStubsProvider: WorkflowServiceStubsProvider,
  private val environmentSchedulerWorkerFactoryVisitor: EnvironmentSchedulerWorkerFactoryVisitor,
  private val resourceSchedulerWorkerFactoryVisitor: ResourceSchedulerWorkerFactoryVisitor,
  private val clock: Clock
) {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  /**
   * Bouncing the workflow triggers it to run right away
   */
  fun resetResourceSupervisor() {
    val workflowClient = workflowClientProvider.get(TEMPORAL_NAMESPACE)
    resourceSchedulerWorkerFactoryVisitor.resetSupervisor(RESOURCE, workflowClient, taskQueueNamer, workflowServiceStubsProvider, clock)
  }

  /**
   * Bouncing the workflow triggers it to run right away
   */
  fun resetEnvironmentSupervisor() {
    val workflowClient = workflowClientProvider.get(TEMPORAL_NAMESPACE)
    environmentSchedulerWorkerFactoryVisitor.resetSupervisor(ENVIRONMENT, workflowClient, taskQueueNamer, workflowServiceStubsProvider, clock)
  }
}
