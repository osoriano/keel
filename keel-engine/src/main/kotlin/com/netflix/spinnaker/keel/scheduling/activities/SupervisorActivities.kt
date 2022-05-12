package com.netflix.spinnaker.keel.scheduling.activities

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.scheduling.ResourceSchedulerService
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.TEMPORAL_NAMESPACE
import com.netflix.spinnaker.keel.scheduling.TemporalClient
import com.netflix.spinnaker.keel.scheduling.WorkerEnvironment
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.temporal.activity.Activity
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityOptions
import io.temporal.api.workflowservice.v1.ListWorkflowExecutionsRequest
import io.temporal.common.RetryOptions
import io.temporal.failure.ApplicationFailure
import io.temporal.workflow.Workflow
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * Activities related to the [SchedulerSupervisor].
 */
@ActivityInterface(namePrefix = "SupervisorActivities-")
interface SupervisorActivities {

  /**
   * Reconciles running schedulers with the known managed resources.
   *
   * This method will start schedulers that are not running, and terminate schedulers for resources that are no
   * longer managed.
   */
  fun reconcileSchedulers(request: ReconcileSchedulersRequest)

  data class ReconcileSchedulersRequest(
    val type: String = "resource"
  )

  companion object {
    fun get(): SupervisorActivities =
      Workflow.newActivityStub(
        SupervisorActivities::class.java,
        ActivityOptions.newBuilder()
          .setTaskQueue(Workflow.getInfo().taskQueue)
          .setStartToCloseTimeout(Duration.ofHours(1))
          .setHeartbeatTimeout(Duration.ofMinutes(3))
          .setRetryOptions(
            RetryOptions.newBuilder()
              .setBackoffCoefficient(1.1)
              .setInitialInterval(Duration.ofMinutes(1))
              .setMaximumInterval(Duration.ofMinutes(10))
              .build()
          )
          .build()
      )
  }
}

@Component
class DefaultSupervisorActivities(
  private val keelRepository: KeelRepository,
  private val resourceSchedulerService: ResourceSchedulerService,
  private val temporalClient: TemporalClient,
  private val workerEnvironment: WorkerEnvironment,
  private val registry: Registry,
  private val featureToggles: FeatureToggles
) : SupervisorActivities {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  override fun reconcileSchedulers(request: SupervisorActivities.ReconcileSchedulersRequest) {
    if (!featureToggles.isEnabled(FeatureToggles.SUPERVISOR_SCHEDULING_CONFIG, false)) {
      log.debug("Supervising via temporal is disabled, skipping.")
      throw ApplicationFailure.newFailure("continuing activity", "continuation")
    }

    val knownResourceUids = mutableListOf<String>()
    var i = 0
    keelRepository.allResources().forEachRemaining {
      resourceSchedulerService.startScheduling(it)
      knownResourceUids.add("resource:${it.uid}")
      maybeHeartbeat(i++)
    }
    log.debug("Found and scheduled ${knownResourceUids.size} resources with ${this.javaClass.simpleName}")

    val listRequest = ListWorkflowExecutionsRequest.newBuilder()
      .setNamespace(TEMPORAL_NAMESPACE)
      .setQuery("WorkflowType = 'ResourceScheduler' AND ExecutionStatus = 'Running' AND WorkerEnv = '${workerEnvironment.get()}'")
      .build()

    i = 0
    temporalClient.iterateWorkflows(listRequest)
      .forEachRemaining {
        val workflowId = it.execution.workflowId
        if (!knownResourceUids.contains(workflowId)) {
          try {
            log.info("Terminating scheduler for unknown resource '$workflowId'")
            temporalClient.terminateWorkflow(TEMPORAL_NAMESPACE, it.execution)
            registry.counter("keel.resource-scheduler.supervisor.terminations").increment()
          } catch (e: StatusRuntimeException) {
            if (e.status.code != Status.Code.NOT_FOUND) {
              log.error("Failed to terminate scheduler for unknown resource '$workflowId'", e)
            }
          }
        }
        maybeHeartbeat(i++)
      }

    throw ApplicationFailure.newFailure("continuation", "expected")
  }

  private fun maybeHeartbeat(i: Int) {
    if (i % 10 == 0) {
      Activity.getExecutionContext().heartbeat("still working")
    }
  }
}
