package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.api.ResourceKind
import com.netflix.spinnaker.keel.persistence.EnvironmentHeader
import com.netflix.spinnaker.keel.scheduling.SchedulingConsts.TEMPORAL_NAMESPACE
import com.netflix.spinnaker.keel.test.resource
import com.netflix.temporal.core.convention.TaskQueueNamer
import com.netflix.temporal.spring.WorkflowClientProvider
import com.netflix.temporal.spring.WorkflowServiceStubsProvider
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.serviceclient.WorkflowServiceStubs
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.core.env.StandardEnvironment
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class TemporalSchedulerServiceTest {

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

  private val env = EnvironmentHeader("keel", "test")

  private lateinit var subject: TemporalSchedulerService

  @BeforeEach
  fun setup() {
    workflowServiceStubs = mockk()
    blockingWorkflowServiceStubs = mockk(relaxed = true)
    every { workflowServiceStubsProvider.forNamespace(TEMPORAL_NAMESPACE) } returns workflowServiceStubs
    every { workflowServiceStubs.blockingStub() } returns blockingWorkflowServiceStubs

    workflowClient = mockk()
    every { workflowClientProvider.get(TEMPORAL_NAMESPACE) } returns workflowClient

    subject = TemporalSchedulerService(workflowClientProvider, workflowServiceStubsProvider, taskQueueNamer, WorkerEnvironment(StandardEnvironment()))
  }

  @Test
  fun `startScheduling should create a new workflow`() {
    val stub = mockk<ResourceScheduler>(relaxUnitFun = true)
    every { workflowClient.newWorkflowStub(ResourceScheduler::class.java, any<WorkflowOptions>()) } returns stub

    subject.startScheduling(res)

    verify { stub.schedule(any()) }
  }

  @Test
  fun `checkNow resource should signal the workflow`() {
    subject.checkResourceNow(res)

    verify {
      blockingWorkflowServiceStubs.signalWorkflowExecution(any())
    }
  }

  @Test
  fun `stopScheduling should terminate workflow if canceling fails`() {
    subject.stopScheduling(res)

    verify {
      blockingWorkflowServiceStubs.terminateWorkflowExecution(any())
    }
  }

  @Test
  fun `should create env workflows`() {
    val stub = mockk<EnvironmentScheduler>(relaxUnitFun = true)
    every { workflowClient.newWorkflowStub(EnvironmentScheduler::class.java, any<WorkflowOptions>()) } returns stub

    subject.startSchedulingEnvironment(env.application, env.name)

    verify { stub.schedule(any()) }
  }

  @Test
  fun `should stop scheduling env workflows`() {
    subject.stopScheduling(env.application, env.name)

    verify {
      blockingWorkflowServiceStubs.terminateWorkflowExecution(any())
    }
  }

  @Test
  fun `checkNow env should signal the workflow`() {
    subject.checkEnvironmentNow(env.application, env.name)

    verify {
      blockingWorkflowServiceStubs.signalWorkflowExecution(any())
    }
  }

  @Test
  fun `workflow id`() {
    // this is a pattern we rely upon, it can't be changed w/o coordinating
    val app = "app"
    val env = "env"
    expectThat(subject.workflowId(app, env)).isEqualTo("environment:$app:$env")
  }
}
