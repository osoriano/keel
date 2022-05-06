/*
 *
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.spinnaker.keel.pause

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.actuation.EnvironmentTaskCanceler
import com.netflix.spinnaker.keel.events.ApplicationActuationPaused
import com.netflix.spinnaker.keel.events.ApplicationActuationResumed
import com.netflix.spinnaker.keel.events.ResourceActuationPaused
import com.netflix.spinnaker.keel.events.ResourceActuationResumed
import com.netflix.spinnaker.keel.persistence.PausedRepository
import com.netflix.spinnaker.keel.persistence.ResourceRepository
import com.netflix.spinnaker.keel.test.resource
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.springframework.context.ApplicationEventPublisher
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class ActuationPauserTests : JUnit5Minutests {
  val awsTest = "test"
  val awsProd = "prod"
  val titusTest = "titusTest"
  val titusProd = "titusProd"
  val testEc2 = "ec2:cluster:$awsTest:pancake"
  val testTitus = "titus:cluster:$titusTest:pancake"
  val prodEc2 = "ec2:cluster:$awsProd:pancake"
  val prodTitus = "titus:cluster:$titusProd:pancake"
  val prodWafflesEc2 = "ec2:cluster:$awsProd:waffles"
  val prodWafflesTitus = "titus:cluster:$titusProd:waffles"
  val prodBiscuitEc2 = "ec2:cluster:$awsProd:biscuit"
  val prodBiscuitTitus = "titus:cluster:$titusProd:biscuit"

  class Fixture {
    val resource1 = resource()
    val resource2 = resource()
    val clock = MutableClock()
    val resourceRepository = mockk<ResourceRepository>()
    val pausedRepository = mockk<PausedRepository>(relaxUnitFun = true)
    val publisher = mockk<ApplicationEventPublisher>(relaxUnitFun = true)
    val environmentTaskCanceler: EnvironmentTaskCanceler = mockk(relaxed = true)
    val spectator: Registry = mockk(relaxed = true)
    val subject = ActuationPauser(resourceRepository, pausedRepository, publisher, environmentTaskCanceler, clock, spectator)
    val user = "keel@keel.io"
  }

  fun tests() = rootContext<Fixture> {
    fixture { Fixture() }

    before {
      every { resourceRepository.get(resource1.id) } returns resource1
      every { resourceRepository.get(resource2.id) } returns resource2
    }

    context("application wide") {
      test("pause is reflected") {
        subject.pauseApplication(resource1.application, user)

        verify { pausedRepository.pauseApplication(resource1.application, user) }
      }

      test("pause event is generated") {
        subject.pauseApplication(resource1.application, user)

        val event = slot<ApplicationActuationPaused>()
        verify(exactly = 1) {
          publisher.publishEvent(capture(event))
        }

        expectThat(event.captured.triggeredBy).isEqualTo(user)

        // no matching ResourceActuationPaused events are generated here as they are dynamically inserted into the
        // list by EventController to account for newly added resources
        verify(exactly = 0) {
          publisher.publishEvent(ofType<ResourceActuationPaused>())
        }
      }

      test("resume is reflected") {
        subject.resumeApplication(resource1.application, user)

        verify { pausedRepository.resumeApplication(resource1.application) }
      }

      test("resume event is generated") {
        subject.resumeApplication(resource1.application, user)

        val event = slot<ApplicationActuationResumed>()
        verify(exactly = 1) {
          publisher.publishEvent(capture(event))
        }

        expectThat(event.captured.triggeredBy).isEqualTo(user)

        verify(exactly = 0) {
          publisher.publishEvent(ofType<ResourceActuationResumed>())
        }
      }

      test("cancel tasks if requested") {
        subject.pauseApplication(resource1.application, user, "pausing", true)
        verify(exactly = 1) {
          environmentTaskCanceler.cancelTasksForApplication(resource1.application, user)
        }
      }
    }

    context("just a resource") {
      test("pause is reflected") {
        subject.pauseResource(resource1.id, user)

        verify { pausedRepository.pauseResource(resource1.id, any()) }
        verify(exactly = 0) { pausedRepository.pauseResource(resource2.id, any()) }
      }

      test("paused event is generated") {
        subject.pauseResource(resource1.id, user)

        val event = slot<ResourceActuationPaused>()
        verify(exactly = 1) { publisher.publishEvent(capture(event)) }

        expectThat(event.captured.triggeredBy).isEqualTo(user)
      }

      test("resume is reflected") {
        subject.resumeResource(resource1.id, user)

        verify { pausedRepository.resumeResource(resource1.id) }
        verify(exactly = 0) { pausedRepository.pauseResource(resource2.id, any()) }
      }

      test("resume event is generated") {
        subject.resumeResource(resource1.id, user)

        val event = slot<ResourceActuationResumed>()
        verify(exactly = 1) { publisher.publishEvent(capture(event)) }

        expectThat(event.captured.triggeredBy).isEqualTo(user)
      }
    }

    context("orchestrating batch pause") {
      before {
        every { resourceRepository.getResourceIdsForClusterName("pancake") } returns
          listOf(prodEc2, prodTitus)
        every { resourceRepository.getResourceIdsForClusterName("waffles") } returns
          listOf(prodWafflesEc2, prodWafflesTitus)
        every { resourceRepository.getResourceIdsForClusterName("biscuits") } returns
          listOf(prodBiscuitEc2, prodBiscuitTitus)
        every { resourceRepository.get(any()) } returns resource1 // for publishing an event
      }

      test("successfully call pause on all clusters") {
        subject.batchPauseClusters(
          user = user,
          clusters = listOf("pancake", "waffles", "biscuits"),
          titusAccount = titusProd,
          ec2Account = awsProd,
          comment = "no breakfast at this time"
        )
        // need to sleep because we launch this in a coroutine
        Thread.sleep(250)

        verify(exactly = 1) { pausedRepository.pauseResource(prodEc2, any(), any()) }
        verify(exactly = 1) { pausedRepository.pauseResource(prodTitus, any(), any()) }
        verify(exactly = 1) { pausedRepository.pauseResource(prodWafflesEc2, any(), any()) }
        verify(exactly = 1) { pausedRepository.pauseResource(prodWafflesTitus, any(), any()) }
        verify(exactly = 1) { pausedRepository.pauseResource(prodBiscuitEc2, any(), any()) }
        verify(exactly = 1) { pausedRepository.pauseResource(prodBiscuitTitus, any(), any()) }
      }

      test("successfully call resume on all clusters") {
        subject.batchResumeClusters(
          user = user,
          clusters = listOf("pancake", "waffles", "biscuits"),
          titusAccount = titusProd,
          ec2Account = awsProd,
          comment = "resume breakfast pronto!"
        )
        // need to sleep because we launch this in a coroutine
        Thread.sleep(250)

        verify(exactly = 1) { pausedRepository.resumeResourceIfSameUser(prodEc2, user) }
        verify(exactly = 1) { pausedRepository.resumeResourceIfSameUser(prodTitus, user) }
        verify(exactly = 1) { pausedRepository.resumeResourceIfSameUser(prodWafflesEc2, user) }
        verify(exactly = 1) { pausedRepository.resumeResourceIfSameUser(prodWafflesTitus, user) }
        verify(exactly = 1) { pausedRepository.resumeResourceIfSameUser(prodBiscuitEc2, user) }
        verify(exactly = 1) { pausedRepository.resumeResourceIfSameUser(prodBiscuitTitus, user) }
      }
    }

    context("batch cluster operations"){
      before {
        every { resourceRepository.getResourceIdsForClusterName(any()) } returns
          listOf(testEc2, testTitus, prodEc2, prodTitus)
        every { resourceRepository.get(any()) } returns resource1 // for publishing an event
      }

      test("pauses the right resources") {
        subject.pauseCluster("pancake", titusTest, awsTest, "me@breakfast", "testing pause")
        verify(exactly = 1) { pausedRepository.pauseResource(testEc2, any(), any()) }
        verify(exactly = 1) { pausedRepository.pauseResource(testTitus, any(), any()) }
        verify(exactly = 0) { pausedRepository.pauseResource(prodEc2, any(), any()) }
        verify(exactly = 0) { pausedRepository.pauseResource(prodTitus, any(), any()) }
      }

      test("resumes the right resources") {
        val user = "me@breakfast"
        subject.resumeCluster("pancake", titusProd, awsProd, user, "testing resume")
        verify(exactly = 0) { pausedRepository.resumeResourceIfSameUser(testEc2, user) }
        verify(exactly = 0) { pausedRepository.resumeResourceIfSameUser(testTitus, user) }
        verify(exactly = 1) { pausedRepository.resumeResourceIfSameUser(prodEc2, user) }
        verify(exactly = 1) { pausedRepository.resumeResourceIfSameUser(prodTitus, user) }
      }
    }
  }
}