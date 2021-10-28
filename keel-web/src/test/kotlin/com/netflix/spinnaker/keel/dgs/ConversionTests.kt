package com.netflix.spinnaker.keel.dgs

import com.netflix.spinnaker.keel.persistence.TaskForResource
import com.netflix.spinnaker.time.MutableClock
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class ConversionTests {

  private val clock = MutableClock()

  @Test
  fun taskNameShortening() {
    val shortName = "Disable extra active server group"
    val task = TaskForResource(
      id = "1",
      name = "$shortName [emburnstest-mmmwaffles-testing-v117 in titustestvpc/us-east-1]",
      resourceId = "1",
      startedAt = clock.instant(),
      endedAt = null,
      artifactVersion = null
    )
    expectThat(task.toDgs().name).isEqualTo(shortName)

    val nameWithoutBrackets = "task without brackets"
    expectThat(task.copy(name = nameWithoutBrackets).toDgs().name).isEqualTo(nameWithoutBrackets)
  }
}
