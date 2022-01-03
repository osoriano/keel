package com.netflix.spinnaker.keel.api

import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isSuccess

internal class EnvironmentChangeReasonTests {

  @Test
  fun `ResourceChange resolves added, removed, and changed resources`() {
    val current = mapOf(
      "resource-1" to 1,
      "resource-2" to 1
    )
    val new = mapOf(
      "resource-1" to 2,
      "resource-3" to 1
    )

    val changeReason = ResourceChange(current, new)

    expectThat(changeReason) {
      get(ResourceChange::added) isEqualTo setOf("resource-3")
      get(ResourceChange::removed) isEqualTo setOf("resource-2")
      get(ResourceChange::changed) isEqualTo setOf("resource-1")
    }
  }

  @Test
  fun `can read an unknown reason`() {
    val mapper = configuredObjectMapper()

    expectCatching {
      mapper.readValue<EnvironmentChangeReason>("""{"reason":"unknown"}""")
    }
      .isSuccess()
      .isA<UnknownChange>()
  }

}
