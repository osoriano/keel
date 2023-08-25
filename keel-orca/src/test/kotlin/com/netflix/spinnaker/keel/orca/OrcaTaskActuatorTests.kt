package com.netflix.spinnaker.keel.orca

import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.TaskStatus
import com.netflix.spinnaker.keel.api.TaskStatus.SUCCEEDED
import com.netflix.spinnaker.keel.api.TaskStatus.TERMINAL
import com.netflix.spinnaker.keel.api.actuation.SubjectType.CONSTRAINT
import com.netflix.spinnaker.keel.api.actuation.SubjectType.RESOURCE
import com.netflix.spinnaker.keel.api.actuation.Task
import com.netflix.spinnaker.keel.core.api.randomUID
import com.netflix.spinnaker.keel.events.ResourceTaskFailed
import com.netflix.spinnaker.keel.events.ResourceTaskSucceeded
import com.netflix.spinnaker.keel.events.TaskCreatedEvent
import com.netflix.spinnaker.keel.persistence.NoSuchResourceId
import com.netflix.spinnaker.keel.persistence.ResourceRepository
import com.netflix.spinnaker.keel.persistence.TaskRecord
import com.netflix.spinnaker.keel.persistence.TaskTrackingRepository
import com.netflix.spinnaker.keel.test.DummyResourceSpec
import com.netflix.spinnaker.keel.test.resource
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.clearAllMocks
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.ResponseBody.Companion.toResponseBody
import org.springframework.context.ApplicationEventPublisher
import retrofit2.HttpException
import retrofit2.Response
import strikt.api.expectThrows
import java.time.Clock
import io.mockk.coEvery as every

internal class OrcaTaskActuatorTests : JUnit5Minutests {

  companion object {
    val clock: Clock = Clock.systemUTC()
    val orcaService: OrcaService = mockk(relaxed = true)
    val publisher: ApplicationEventPublisher = mockk(relaxed = true)
    val taskTrackingRepository: TaskTrackingRepository = mockk(relaxUnitFun = true)
    val resourceRepository: ResourceRepository = mockk()
    val resource: Resource<DummyResourceSpec> = resource()

    val taskResourceRecord = TaskRecord(
      id = "123",
      subjectType = RESOURCE,
      application = "fnord",
      environmentName = "prod",
      resourceId = resource.id,
      name = "upsert server group"
    )

    val taskConstraintRecord = TaskRecord(
      id = "123",
      subjectType = CONSTRAINT,
      application = "fnord",
      environmentName = "prod",
      resourceId = null,
      name = "canary constraint"
    )

    val task = Task(
      id = "123",
      name = "upsert server group"
    )
  }

  data class OrcaTaskActuatorFixture(
    val event: TaskCreatedEvent
  ) {
    val subject: OrcaTaskActuator = OrcaTaskActuator(
      taskTrackingRepository,
      orcaService,
      resourceRepository,
      publisher,
      clock
    )
  }

  fun orcaTaskActuatorTests() = rootContext<OrcaTaskActuatorFixture> {
    fixture {
      OrcaTaskActuatorFixture(
        event = TaskCreatedEvent(taskResourceRecord)
      )
    }

    context("a new orca task is being stored") {
      before {
        every { resourceRepository.get(resource.id) } returns resource
      }

      after {
        clearAllMocks()
      }

      test("do not process running orca tasks") {
        every {
          orcaService.getOrchestrationExecution(event.taskRecord.id)
        } returns executionDetailResponse(event.taskRecord.id)

        runBlocking {
          subject.checkTask(event.taskRecord)
        }

        verify(exactly = 0) { publisher.publishEvent(any()) }
        verify(exactly = 0) { taskTrackingRepository.store(any()) }
        verify(exactly = 0) { taskTrackingRepository.delete(any()) }
      }

      test("a task ended with succeeded status") {
        every {
          orcaService.getOrchestrationExecution(event.taskRecord.id)
        } returns executionDetailResponse(event.taskRecord.id, SUCCEEDED)

        runBlocking {
          subject.checkTask(event.taskRecord)
        }

        verify(exactly = 1) { publisher.publishEvent(ofType<ResourceTaskSucceeded>()) }
        verify { taskTrackingRepository.delete(event.taskRecord.id) }
      }

      test("a task is ended with a failure status with no error") {
        every {
          orcaService.getOrchestrationExecution(event.taskRecord.id)
        } returns executionDetailResponse(event.taskRecord.id, TERMINAL)

        runBlocking {
          subject.checkTask(event.taskRecord)
        }

        verify(exactly = 1) { publisher.publishEvent(ofType<ResourceTaskFailed>()) }
        verify { taskTrackingRepository.delete(event.taskRecord.id) }
      }

      test("a task is ended with a failure status with a general exception") {
        every {
          orcaService.getOrchestrationExecution(event.taskRecord.id)
        } returns executionDetailResponse(
          event.taskRecord.id,
          TERMINAL,
          OrcaExecutionStages(listOf(orcaContext(exception = orcaExceptions())))
        )

        runBlocking {
          subject.checkTask(event.taskRecord)
        }

        verify(exactly = 1) { publisher.publishEvent(ofType<ResourceTaskFailed>()) }
        verify { taskTrackingRepository.delete(event.taskRecord.id) }
      }

      test("a task is ended with a failure status with a kato exception") {
        every {
          orcaService.getOrchestrationExecution(event.taskRecord.id)
        } returns executionDetailResponse(
          event.taskRecord.id,
          TERMINAL,
          OrcaExecutionStages(listOf(orcaContext(katoException = clouddriverExceptions())))
        )

        runBlocking {
          subject.checkTask(event.taskRecord)
        }

        verify(exactly = 1) { publisher.publishEvent(ofType<ResourceTaskFailed>()) }
        verify { taskTrackingRepository.delete(event.taskRecord.id) }
      }

      test("a TaskCreatedEvent is handled") {
        runBlocking {
          subject.onTaskEvent(event)
        }

        verify { taskTrackingRepository.store(event.taskRecord) }
      }
    }

    context("resource not found") {
      before {
        every { resourceRepository.get(resource.id) } throws NoSuchResourceId(resource.id)
        every {
          orcaService.getOrchestrationExecution(event.taskRecord.id)
        } returns executionDetailResponse(event.taskRecord.id, SUCCEEDED)
      }

      after {
        clearAllMocks()
      }

      test("task record is removed") {
        runBlocking {
          subject.checkTask(event.taskRecord)
        }

        verify { taskTrackingRepository.delete(event.taskRecord.id) }
      }
    }

    context("cannot determinate task status since orca returned a not found exception for the task id") {
      before {
        every { resourceRepository.get(resource.id) } returns resource
        every {
          orcaService.getOrchestrationExecution(event.taskRecord.id)
        } throws RETROFIT_NOT_FOUND
      }

      after {
        clearAllMocks()
      }

      test("task record was removed from the table") {
        runBlocking {
          subject.checkTask(event.taskRecord)
        }

        verify(exactly = 0) { publisher.publishEvent(any()) }
        verify { taskTrackingRepository.delete(event.taskRecord.id) }
      }
    }

    context("cannot determinate task status since orca returned an exception for the task id") {
      before {
        every { resourceRepository.get(resource.id) } returns resource
        every {
          orcaService.getOrchestrationExecution(event.taskRecord.id)
        } throws Exception()
      }

      after {
        clearAllMocks()
      }

      test("an exception is thrown and the task was not deleted") {
        runBlocking {
          expectThrows<Exception> {
            subject.checkTask(event.taskRecord)
          }
        }

        verify(exactly = 0) { publisher.publishEvent(any()) }
        verify(exactly = 0) { taskTrackingRepository.delete(any()) }
      }
    }

    context("constraint events") {
      deriveFixture {
        copy(event = TaskCreatedEvent(taskConstraintRecord))
      }

      before {
        every {
          orcaService.getOrchestrationExecution(event.taskRecord.id)
        } returns executionDetailResponse(event.taskRecord.id, SUCCEEDED)
      }

      after {
        clearAllMocks()
      }

      test("do not process tasks which are constraint and not resources") {
        runBlocking {
          subject.checkTask(event.taskRecord)
        }
        verify(exactly = 0) { publisher.publishEvent(any()) }
        verify { taskTrackingRepository.delete(event.taskRecord.id) }
      }
    }
  }

  private fun executionDetailResponse(
    id: String = randomUID().toString(),
    status: TaskStatus = TaskStatus.RUNNING,
    execution: OrcaExecutionStages = OrcaExecutionStages(emptyList())
  ) =
    ExecutionDetailResponse(
      id = id,
      name = "fnord",
      application = "fnord",
      buildTime = clock.instant(),
      startTime = clock.instant(),
      endTime = when (status.isIncomplete()) {
        true -> null
        false -> clock.instant()
      },
      status = status,
      execution = execution
    )

  private fun orcaExceptions() =
    OrcaException(
      details = GeneralErrorsDetails(
        errors =
          listOf("Too many retries.  10 attempts have been made to bake ... its probably not going to happen."),
        stackTrace = "",
        responseBody = "",
        kind = "",
        error = ""
      ),
      exceptionType = "",
      shouldRetry = false
    )

  private fun orcaContext(
    exception: OrcaException? = null,
    katoException: List<Map<String, Any>>? = emptyList()
  ): Map<String, Any> =
    mapOf(
      "context" to
        OrcaContext(
          exception = exception,
          clouddriverException = katoException
        )
    )

  private fun clouddriverExceptions():
    List<Map<String, Any>> = (
      listOf(
        mapOf(
          "exception" to ClouddriverException(
            cause = "",
            message = "The following security groups do not exist: 'keeldemo-main-elb' in 'test' vpc-46f5a223",
            operation = "",
            type = "EXCEPTION"
          )
        )
      )
      )

  val RETROFIT_NOT_FOUND = HttpException(
    Response.error<Any>(404, "".toResponseBody("application/json".toMediaTypeOrNull()))
  )
}
