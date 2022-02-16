package com.netflix.spinnaker.keel.telemetry

import com.netflix.spectator.api.Counter
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Tag
import com.netflix.spectator.api.Timer
import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.api.ResourceStatus.HAPPY
import com.netflix.spinnaker.keel.api.ResourceStatusSnapshot
import com.netflix.spinnaker.keel.api.events.ArtifactVersionMarkedDeploying
import com.netflix.spinnaker.keel.events.ArtifactDeployedNotification
import com.netflix.spinnaker.keel.events.ResourceValid
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.telemetry.TelemetryListener.Companion.ARTIFACT_DELAY
import com.netflix.spinnaker.keel.test.deliveryConfig
import com.netflix.spinnaker.keel.test.dockerArtifact
import com.netflix.spinnaker.keel.test.titusCluster
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.fail
import strikt.api.Assertion
import strikt.api.expectThat
import strikt.assertions.any
import strikt.assertions.isEqualTo
import strikt.assertions.one
import java.time.Duration
import java.util.concurrent.Executors

internal class TelemetryListenerTests : JUnit5Minutests {
  private val repository = mockk<KeelRepository>()
  private val registry = spyk<Registry>(NoopRegistry())
  private val counter = mockk<Counter>(relaxUnitFun = true)
  private val clock = MutableClock()
  private val timer = mockk<Timer>()
  private val deliveryConfig = deliveryConfig(resource = titusCluster(), artifact = dockerArtifact())
  private val environment = deliveryConfig.environments.first()
  private val resource = environment.resources.first()
  private val artifact = deliveryConfig.artifacts.first()
  private val resourceValidEvent = ResourceValid(resource, clock)
  private val featureToggles = mockk<FeatureToggles> { every { isEnabled(any(), any()) } returns false }

  fun tests() = rootContext<TelemetryListener> {
    fixture {
      TelemetryListener(registry, clock, repository, featureToggles, emptyList(), emptyList(), Executors.newSingleThreadExecutor())
    }

    before {
      every { registry.timer(any(), any<Iterable<Tag>>()) } returns timer
      every { timer.record(any<Duration>()) } just runs
      every { repository.wasSuccessfullyDeployedTo(any(),any(),any(),any()) } returns false
    }

    context("successful metric submission") {
      test("increments an Atlas counter") {
        every { registry.counter(any(), any<List<Tag>>()) } returns counter

        onResourceChecked(resourceValidEvent)

        verify {
          counter.increment()
        }
      }

      test("tags the counter") {
        val id = slot<String>()
        val tags = slot<List<Tag>>()
        every {
          registry.counter(capture(id), capture(tags))
        } returns counter

        onResourceChecked(resourceValidEvent)

        expectThat(id.captured).isEqualTo("keel.resource.checked")
        expectThat(tags.captured) {
          any {
            key().isEqualTo("resourceKind")
            value().isEqualTo(resourceValidEvent.kind.toString())
          }
          any {
            key().isEqualTo("resourceId")
            value().isEqualTo(resourceValidEvent.id)
          }
          any {
            key().isEqualTo("resourceApplication")
            value().isEqualTo(resourceValidEvent.application)
          }
          any {
            key().isEqualTo("resourceState")
            value().isEqualTo(resourceValidEvent.state.name)
          }
        }
      }
    }

    context("metric submission fails") {
      before {
        every { counter.id() } returns NoopRegistry().createId("whatever")
        every { counter.increment() } throws IllegalStateException("Somebody set up us the bomb")
      }

      test("does not propagate exception") {
        try {
          onResourceChecked(resourceValidEvent)
        } catch (ex: Exception) {
          fail { "Did not expect an exception but caught: ${ex.message}" }
        }
      }
    }

    context("on artifact version marked deploying") {
      before {
        every {
          repository.getArtifactVersion(any(), any())
        } returns artifact.toArtifactVersion("version", storedAt = clock.instant())
      }

      context("when version is approved") {
        before {
          clearMocks(registry, timer, answers = false)
          every { repository.getApprovedAt(any(), any(), any(), any()) } returns clock.instant()
          every { repository.getPinnedAt(any(), any(), any(), any()) } returns null
          every { repository.constraintStateFor(any(), any(), any(), any()) } returns emptyList()

          onArtifactVersionMarkedDeploying(
            ArtifactVersionMarkedDeploying(deliveryConfig, artifact, "version", environment, clock.instant())
          )
        }

        test("records delay since version approved") {
          verifyArtifactDelayRecorded("deployment", "approved")
        }

        test("records delay since version stored") {
          verifyArtifactDelayRecorded("deployment", "stored")
        }
      }

      context("when version is pinned") {
        before {
          clearMocks(registry, timer, answers = false)
          every { repository.getApprovedAt(any(), any(), any(), any()) } returns null
          every { repository.getPinnedAt(any(), any(), any(), any()) } returns clock.instant()

          onArtifactVersionMarkedDeploying(
            ArtifactVersionMarkedDeploying(deliveryConfig, artifact, "version", environment, clock.instant())
          )
        }

        test("records delay since version pinned") {
          verifyArtifactDelayRecorded("deployment", "pinned")
        }

        test("records delay since version stored") {
          verifyArtifactDelayRecorded("deployment", "stored")
        }
      }
    }

    context("on artifact version marked deployed") {
      before {
        clearMocks(registry, timer, answers = false)
        every {
          repository.getResourceStatus(resource.id)
        } returns ResourceStatusSnapshot(HAPPY, clock.instant())

        onArtifactVersionMarkedDeployed(
          ArtifactDeployedNotification(deliveryConfig, "version", artifact, environment)
        )
      }

      test("records delay since resource happy") {
        verifyArtifactDelayRecorded("resourceHappy")
      }
    }
  }

  private fun verifyArtifactDelayRecorded(delayType: String, action: String? = null) {
    val tags = mutableListOf<Iterable<Tag>>()

    verify { registry.timer(ARTIFACT_DELAY, capture(tags)) }
    verify { timer.record(any<Duration>()) }

    expectThat(tags).one {
      one {
        get { key() }.isEqualTo("delayType")
        get { value() }.isEqualTo(delayType)
      }
      one {
        get { key() }.isEqualTo("artifactType")
      }
      one {
        get { key() }.isEqualTo("artifactName")
      }
      if (action != null) {
        one {
          get { key() }.isEqualTo("action")
          get { value() }.isEqualTo(action)
        }
      }
    }
  }
}

fun Assertion.Builder<Tag>.key() = get { key() }
fun Assertion.Builder<Tag>.value() = get { value() }
