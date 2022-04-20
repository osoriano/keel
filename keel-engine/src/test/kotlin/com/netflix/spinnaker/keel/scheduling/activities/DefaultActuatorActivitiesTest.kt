package com.netflix.spinnaker.keel.scheduling.activities

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.keel.actuation.ResourceActuator
import com.netflix.spinnaker.keel.api.ResourceKind
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.NoSuchResourceId
import com.netflix.spinnaker.keel.scheduling.TEMPORAL_CHECKER
import com.netflix.spinnaker.keel.telemetry.ResourceCheckStarted
import com.netflix.spinnaker.keel.telemetry.ResourceLoadFailed
import com.netflix.spinnaker.keel.test.resource
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.context.ApplicationEventPublisher
import java.time.Clock
import java.time.Instant
import java.time.ZoneId

class DefaultActuatorActivitiesTest {

  private val keelRepository: KeelRepository = mockk()
  private val resourceActuator: ResourceActuator = mockk(relaxUnitFun = true)
  private val publisher: ApplicationEventPublisher = mockk(relaxed = true)
  private val clock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault())
  private val spectator = NoopRegistry()

  private val subject = DefaultActuatorActivities(keelRepository, resourceActuator, publisher, clock, spectator)

  @Test
  fun `should check known resource`() {
    val res = resource(
      kind = ResourceKind.parseKind("ec2/security-group@v1"),
      id = "ec2:security-group:prod:ap-south-1:keel-sg",
      application = "keel"
    )
    every { keelRepository.getResource("ec2:security-group:prod:ap-south-1:keel-sg") } returns res

    subject.checkResource(ActuatorActivities.CheckResourceRequest("ec2:security-group:prod:ap-south-1:keel-sg"))

    coVerify(timeout = 500) {
      publisher.publishEvent(ResourceCheckStarted(res, checker = TEMPORAL_CHECKER))
      resourceActuator.checkResource(res)
    }
  }

  @Test
  fun `should do nothing on unknown resource`() {
    val ex = NoSuchResourceId("id")
    every { keelRepository.getResource(any()) } throws ex

    subject.checkResource(ActuatorActivities.CheckResourceRequest("id"))

    verify {
      publisher.publishEvent(ResourceLoadFailed(ex))
    }
  }
}
