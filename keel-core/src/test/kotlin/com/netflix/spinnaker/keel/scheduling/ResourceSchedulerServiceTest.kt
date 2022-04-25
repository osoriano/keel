package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.api.ResourceKind
import com.netflix.spinnaker.keel.test.resource
import com.netflix.temporal.core.convention.TaskQueueNamer
import com.netflix.temporal.spring.WorkflowClientProvider
import com.netflix.temporal.spring.WorkflowServiceStubsProvider
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.workflow.v1.WorkflowExecutionInfo
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.serviceclient.WorkflowServiceStubs
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment
import strikt.api.expectThat
import strikt.assertions.isFalse
import strikt.assertions.isTrue

class ResourceSchedulerServiceTest {

  private val workflowClientProvider: WorkflowClientProvider = mockk()
  private val workflowServiceStubsProvider: WorkflowServiceStubsProvider = mockk()
  private val taskQueueNamer: TaskQueueNamer = TaskQueueNamer { name -> name }

  private lateinit var workflowServiceStubs: WorkflowServiceStubs
  private lateinit var blockingWorkflowServiceStubs: WorkflowServiceGrpc.WorkflowServiceBlockingStub
  private lateinit var workflowClient: WorkflowClient

  private val res = resource(
    kind = ResourceKind.parseKind("ec2/security-group@v1"),
    id = "ec2:security-group:prod:ap-south-1:keel-sg",
    application = "keel"
  )

  @BeforeEach
  fun setup() {
    workflowServiceStubs = mockk()
    blockingWorkflowServiceStubs = mockk(relaxed = true)
    every { workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE) } returns workflowServiceStubs
    every { workflowServiceStubs.blockingStub() } returns blockingWorkflowServiceStubs

    workflowClient = mockk()
    every { workflowClientProvider.get(TEMPORAL_NAMESPACE) } returns workflowClient
  }

  @Test
  fun `isScheduling should no-op when disabled`() {
    val subject = getSubject(isEnabled = false)
    expectThat(subject.isScheduling(res)).isFalse()
  }

  @Test
  fun `isScheduling should return whether or not an application is currently scheduled by temporal`() {
    var subject = getSubject(isEnabled = false)
    expectThat(subject.isScheduling("keel")).isFalse()

    subject = getSubject(isEnabled = true)
    expectThat(subject.isScheduling("keel")).isTrue()
  }

  @Test
  fun `isScheduling should return whether or not a resource is currently scheduled by temporal`() {
    val subject = getSubject()

    every { blockingWorkflowServiceStubs.describeWorkflowExecution(any()) } throws StatusRuntimeException(Status.NOT_FOUND)
    expectThat(subject.isScheduling(res)).describedAs("not found").isFalse()
    verify { blockingWorkflowServiceStubs.describeWorkflowExecution(any()) }

    every { blockingWorkflowServiceStubs.describeWorkflowExecution(any()) } returns DescribeWorkflowExecutionResponse.newBuilder()
      .setWorkflowExecutionInfo(
        WorkflowExecutionInfo.newBuilder()
          .setStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING)
          .build()
      )
      .build()
    expectThat(subject.isScheduling(res)).describedAs("running").isTrue()

    every { blockingWorkflowServiceStubs.describeWorkflowExecution(any()) } returns DescribeWorkflowExecutionResponse.newBuilder()
      .setWorkflowExecutionInfo(
        WorkflowExecutionInfo.newBuilder()
          .setStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW)
          .build()
      )
      .build()
    expectThat(subject.isScheduling(res)).describedAs("continued as new").isTrue()

    every { blockingWorkflowServiceStubs.describeWorkflowExecution(any()) } returns DescribeWorkflowExecutionResponse.newBuilder()
      .setWorkflowExecutionInfo(
        WorkflowExecutionInfo.newBuilder()
          .setStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED)
          .build()
      )
      .build()
    expectThat(subject.isScheduling(res)).describedAs("failed").isFalse()
  }

  @Test
  fun `startScheduling should no-op when disabled`() {
    val subject = getSubject(isEnabled = false)
    subject.startScheduling(res)
    verify(exactly = 0) {
      workflowClientProvider.get(any())
    }
  }

  @Test
  fun `startScheduling should create a new workflow`() {
    val stub = mockk<ResourceScheduler>(relaxUnitFun = true)
    every { workflowClient.newWorkflowStub(ResourceScheduler::class.java, any<WorkflowOptions>()) } returns stub

    val subject = getSubject()
    subject.startScheduling(res)

    verify { stub.schedule(any()) }
  }

  @Test
  fun `stopScheduling should run when disabled`() {
    // because we need this to flip back and forth between temporal and normal scheduling
    val subject = getSubject(isEnabled = false)
    subject.stopScheduling(res)
    verify(exactly = 1) {
      blockingWorkflowServiceStubs.terminateWorkflowExecution(any())
    }
  }

  @Test
  fun `stopScheduling should terminate workflow if canceling fails`() {
    val subject = getSubject()

    subject.stopScheduling(res)

    verify {
      blockingWorkflowServiceStubs.terminateWorkflowExecution(any())
    }
  }

  private fun getSubject(isEnabled: Boolean = true): ResourceSchedulerService {
    val env = StandardEnvironment()
    if (isEnabled) {
      env.propertySources.addFirst(
        MapPropertySource(
          "test",
          mapOf(
            "keel.resource-scheduler.applications-allowed" to "keel"
          )
        )
      )
    }
    return ResourceSchedulerService(workflowClientProvider, workflowServiceStubsProvider, taskQueueNamer, env)
  }
}
