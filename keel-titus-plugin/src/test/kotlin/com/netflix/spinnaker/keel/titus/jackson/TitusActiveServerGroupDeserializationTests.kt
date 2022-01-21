package com.netflix.spinnaker.keel.titus.jackson

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.keel.api.titus.TitusServerGroup
import com.netflix.spinnaker.keel.clouddriver.model.*
import com.netflix.spinnaker.keel.jackson.KeelApiModule
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.assertions.*

class TitusActiveServerGroupDeserializationTests {

  val mapper: ObjectMapper = configuredObjectMapper()
    .registerModule(KeelApiModule)
    .registerModule(KeelTitusApiModule)

  @Test
  fun `can deserialize a server group with a scaling policy`() {
    val expected = TitusScaling.Policy.TargetPolicy(
      targetPolicyDescriptor = TargetPolicyDescriptor(
        targetValue = 2.0,
        scaleOutCooldownSec = 300,
        scaleInCooldownSec = 300,
        disableScaleIn = false,
        customizedMetricSpecification = CustomizedMetricSpecificationModel(
          metricName = "AverageCPUUtilization",
          namespace = "NFLX/EPIC",
          statistic = "Average"
        )
      )
    )

    expectCatching {
      readResource<TitusActiveServerGroup>("/titus-active-server-group-with-scaling.json")
    }
      .isSuccess()
      .get { scalingPolicies }
      .hasSize(1)
      .first().get { policy } isEqualTo expected
  }

  @Test
  fun `can deserialize a server group with multiple scaling policies`() {
    expectCatching {
      readResource<TitusActiveServerGroup>("/titus-active-server-group-with-complex-scaling.json")
    }
      .isSuccess()
      .get { scalingPolicies }
      .hasSize(3)
  }

  @Test
  fun `can deserialize platform sidecars`() {
    expectCatching {
      readResource<TitusActiveServerGroup>("/titus-active-server-group-with-platform-sidecars.json")
    }
      .isSuccess()
      .get { platformSidecars }
      .hasSize(2)
      .and {
        this[0].isEqualTo(PlatformSidecar("foo", "beta"))
        this[1].isEqualTo(PlatformSidecar("bar", "stable", mapOf("speed" to "turbo")))
      }
  }

  private inline fun <reified T> readResource(path: String) =
    mapper.readValue<T>(resource(path))

  private fun resource(path: String) = checkNotNull(javaClass.getResource(path)) {
    "Resource $path not found"
  }
}
