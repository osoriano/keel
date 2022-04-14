package com.netflix.spinnaker.keel.events

import com.netflix.spinnaker.keel.api.ResourceStatus.HAPPY
import com.netflix.spinnaker.keel.persistence.ResourceRepository
import com.netflix.spinnaker.keel.services.ResourceStatusService
import com.netflix.spinnaker.keel.test.resource
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import java.time.Clock

class ResourceHistoryListenerTests : JUnit5Minutests {
  object Fixture {
    val resourceRepository: ResourceRepository = mockk()
    val resourceStatusService: ResourceStatusService = mockk()
    val listener = ResourceHistoryListener(resourceRepository, resourceStatusService)
    val resource = resource()
    val resourceValidEvent = ResourceValid(resource)
  }

  fun tests() = rootContext<Fixture> {
    fixture {
      Fixture
    }

    context("resource event received") {
      before {
        every {
          resourceRepository.appendHistory(any() as ResourceEvent)
        } just Runs

        every {
          resourceRepository.lastEvent(any())
        } returns ResourceDeltaDetected(resource, mapOf("some" to "change"), Clock.systemUTC())

        every {
          resourceRepository.updateStatus(any(), any())
        } just Runs

        every {
          resourceStatusService.getStatusFromHistory(any())
        } returns HAPPY

        listener.onResourceEvent(resourceValidEvent)
      }

      test("event is persisted") {
        verify(exactly = 1) {
          resourceRepository.appendHistory(resourceValidEvent)
        }
      }

      test("resource status is updated") {
        verify {
          resourceRepository.updateStatus(resourceValidEvent.ref, HAPPY)
        }
      }
    }
  }
}