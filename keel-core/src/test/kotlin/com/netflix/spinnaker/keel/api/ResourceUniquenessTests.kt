package com.netflix.spinnaker.keel.api

import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.hasSize

internal class ResourceUniquenessTests {
  @Test
  fun `can have resources with identical specs in different environments`() {
    val resource1 = Resource<DummyNoIdResourceSpec>(
      kind = ResourceKind.parseKind("test/whatever@v1"),
      metadata = mapOf("id" to "1", "application" to "fnord"),
      spec = DummyNoIdResourceSpec()
    )
    val resource2 = resource1.copy(
      metadata = resource1.metadata + ("id" to "2")
    )

    val deliveryConfig = DeliveryConfig(
      application = "fnord",
      name = "fnord-manifest",
      serviceAccount = "fzlem@netflix.com",
      environments = setOf(
        Environment(
          name = "env1",
          resources = setOf(resource1)
        ),
        Environment(
          name = "env2",
          resources = setOf(resource2)
        )
      )
    )

    expectThat(deliveryConfig.resources).hasSize(2)
  }
}

private data class DummyNoIdResourceSpec(
  val data: String = "OxACAB"
) : ResourceSpec
