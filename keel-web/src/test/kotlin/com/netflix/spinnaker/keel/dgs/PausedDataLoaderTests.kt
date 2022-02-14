package com.netflix.spinnaker.keel.dgs

import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.pause.Pause
import com.netflix.spinnaker.keel.pause.PauseScope.APPLICATION
import com.netflix.spinnaker.keel.pause.PauseScope.RESOURCE
import com.netflix.spinnaker.time.MutableClock
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.assertions.containsKey
import strikt.assertions.containsKeys
import strikt.assertions.doesNotContainKey
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isNull

class PausedDataLoaderTests {
  val actuationPauser: ActuationPauser = mockk()
  val clock = MutableClock()
  val subject = PausedDataLoader(actuationPauser)

  val appKey = PausedKey(APPLICATION, "hi")
  val resourceKey = PausedKey(RESOURCE, "hi-resource")

  val applicationPause = Pause(
    scope = APPLICATION,
    name = appKey.name,
    pausedAt = clock.instant(),
    pausedBy = "me",
    comment = "just testin'"
  )

  val resourcePause = Pause(
    scope = RESOURCE,
    name = resourceKey.name,
    pausedAt = clock.instant(),
    pausedBy = "me",
    comment = "just testin'"
  )

  @Test
  fun `bulk load with nothing paused produces nothing`() {
    every { actuationPauser.bulkPauseInfo(any(), any()) } returns emptyList()

    val result = subject.load(mutableSetOf(appKey))
    expect {
      that(result).hasSize(0)
    }
  }

  @Test
  fun `bulk load with applications produces applications`() {
    every { actuationPauser.bulkPauseInfo(APPLICATION, listOf(appKey.name)) } returns listOf(
      applicationPause
    )

    val result = subject.load(mutableSetOf(appKey))
    expect {
      that(result).hasSize(1)
      that(result).containsKey(appKey)
      that(result).get { get(appKey) }.isEqualTo(applicationPause.toDgsPaused())
    }
  }

  @Test
  fun `bulk load with resources produces resources`() {
    every { actuationPauser.bulkPauseInfo(RESOURCE, listOf(resourceKey.name)) } returns listOf(
      resourcePause
    )

    val result = subject.load(mutableSetOf(resourceKey))
    expect {
      that(result).hasSize(1)
      that(result).containsKey(resourceKey)
      that(result).get { get(resourceKey) }.isEqualTo(resourcePause.toDgsPaused())
    }
  }

  @Test
  fun `bulk load with applications and resources produces correct info`() {
    every { actuationPauser.bulkPauseInfo(APPLICATION, any()) } returns listOf(
      applicationPause
    )
    every { actuationPauser.bulkPauseInfo(RESOURCE, any()) } returns listOf(
      resourcePause
    )
    val newKey = appKey.copy(name = "woah")

    val result = subject.load(mutableSetOf(resourceKey, appKey, newKey))
    expect {
      that(result).hasSize(2)
      that(result).containsKeys(appKey, resourceKey)
      that(result).doesNotContainKey(newKey)
      that(result).get { get(appKey) }.isEqualTo(applicationPause.toDgsPaused())
      that(result).get { get(resourceKey) }.isEqualTo(resourcePause.toDgsPaused())
    }
  }
}
