package com.netflix.spinnaker.keel.scheduling.activities

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.api.ResourceKind
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.ResourceHeader
import com.netflix.spinnaker.keel.scheduling.ResourceSchedulerService
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.TEMPORAL_NAMESPACE
import com.netflix.spinnaker.keel.scheduling.TemporalClient
import com.netflix.spinnaker.keel.scheduling.WorkerEnvironment
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.workflow.v1.WorkflowExecutionInfo
import io.temporal.failure.ActivityFailure
import io.temporal.failure.ApplicationFailure
import io.temporal.testing.TestActivityEnvironment
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectThrows
import strikt.assertions.isA
import strikt.assertions.isFalse

class SupervisorActivitiesTest {

  private lateinit var testActivityEnvironment: TestActivityEnvironment

  private val keelRepository: KeelRepository = mockk(relaxed = true) {
    every { allResources() } returns listOf(
      ResourceHeader("1", ResourceKind("group", "kind", "1"), "1", "foo")
    ).iterator()
  }

  private val featureToggles: FeatureToggles = mockk(relaxed = true) {
    every { isEnabled(FeatureToggles.SUPERVISOR_SCHEDULING_CONFIG, any()) } returns true
  }

  private val resourceSchedulerService: ResourceSchedulerService = mockk(relaxed = true)

  private val temporalClient: TemporalClient = mockk(relaxed = true)

  private val workerEnvironment: WorkerEnvironment = mockk {
    every { get() } returns WorkerEnvironment.Type.LAPTOP
  }
  private val registry = NoopRegistry()

  private lateinit var subject: SupervisorActivities

  @BeforeEach
  fun before() {
    testActivityEnvironment = TestActivityEnvironment.newInstance()
    testActivityEnvironment.registerActivitiesImplementations(
      DefaultSupervisorActivities(keelRepository, resourceSchedulerService, temporalClient, workerEnvironment, registry, featureToggles)
    )
    subject = testActivityEnvironment.newActivityStub(SupervisorActivities::class.java)
  }

  @Test
  fun `should terminate unmanaged resource schedulers`() {
    val exec1 = WorkflowExecutionInfo.newBuilder()
      .setExecution(
        WorkflowExecution.newBuilder()
          .setWorkflowId("resource:1")
          .build()
      )
      .build()
    val exec2 = WorkflowExecutionInfo.newBuilder()
      .setExecution(
        WorkflowExecution.newBuilder()
          .setWorkflowId("resource:2")
          .build()
      )
      .build()

    every { temporalClient.iterateWorkflows(any()) } answers { listOf(exec1, exec2).iterator() }
    every { temporalClient.terminateWorkflow(any(), any()) } just Runs

    expectThrows<ActivityFailure> { subject.reconcileSchedulers(SupervisorActivities.ReconcileSchedulersRequest()) }
      .get { cause }.isA<ApplicationFailure>().get { isNonRetryable }.isFalse()

    verify(exactly = 1) { temporalClient.terminateWorkflow(TEMPORAL_NAMESPACE, exec2.execution) }
  }

  @Test
  fun `should start managed resource schedulers`() {
    expectThrows<ActivityFailure> { subject.reconcileSchedulers(SupervisorActivities.ReconcileSchedulersRequest()) }
    verify(exactly = 1) { resourceSchedulerService.startScheduling(any<ResourceHeader>()) }
  }

  @Test
  fun `should do nothing if supervising is not enabled`() {
    every { featureToggles.isEnabled(FeatureToggles.SUPERVISOR_SCHEDULING_CONFIG, any()) } returns false

    expectThrows<ActivityFailure> { subject.reconcileSchedulers(SupervisorActivities.ReconcileSchedulersRequest()) }
    verify(exactly = 0) { resourceSchedulerService.startScheduling(any<ResourceHeader>()) }
  }
}
