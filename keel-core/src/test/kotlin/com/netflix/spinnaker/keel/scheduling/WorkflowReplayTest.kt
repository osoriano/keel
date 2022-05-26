package com.netflix.spinnaker.keel.scheduling

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.netflix.spinnaker.keel.scheduling.activities.ActuatorActivities
import com.netflix.spinnaker.keel.scheduling.activities.SchedulingConfigActivities
import io.mockk.mockk
import io.temporal.client.WorkflowClientOptions
import io.temporal.common.converter.*
import io.temporal.testing.TestWorkflowExtension
import io.temporal.worker.Worker
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

/**
 * If workflow history needs to be refreshed, you can obtain compatible history via:
 *
 * metatron curl -a keel -v "https://localhost:8087/admin/temporalWorkflowHistory?workflowId={workflowId}&namespace=managed-delivery"
 */
class WorkflowReplayTest {

  private val objectMapper: ObjectMapper = ObjectMapper().registerKotlinModule()
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    .registerKotlinModule()
    .registerModule(JavaTimeModule())

  private val dataConverter: DataConverter = DefaultDataConverter(
    NullPayloadConverter(),
    ByteArrayPayloadConverter(),
    ProtobufJsonPayloadConverter(),
    JacksonJsonPayloadConverter(objectMapper)
  )

  private val workflowClientOptions: WorkflowClientOptions = WorkflowClientOptions.newBuilder()
    .setDataConverter(dataConverter)
    .build()

  @JvmField
  @RegisterExtension
  val workflowExtension: TestWorkflowExtension = TestWorkflowExtension.newBuilder()
    .setWorkflowTypes(
      ResourceSchedulerImpl::class.java,
      EnvironmentSchedulerImpl::class.java
    )
    .setActivityImplementations(
      mockk<SchedulingConfigActivities>(relaxed = true),
      mockk<ActuatorActivities>(relaxed = true)
    )
    .setWorkflowClientOptions(workflowClientOptions)
    .build()

  @Test
  fun `replay history - resource scheduler`(worker: Worker) {
    worker.replayWorkflowExecution(javaClass.getResource("/resource-scheduler-history.json").readText())
  }

  @Test
  fun `replay history - environment scheduler`(worker: Worker) {
    worker.replayWorkflowExecution(javaClass.getResource("/environment-scheduler-history.json").readText())
  }
}
