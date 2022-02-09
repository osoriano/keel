package com.netflix.spinnaker.keel.titus.jackson

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.keel.api.titus.ResourcesSpec
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.api.titus.TitusServerGroup
import com.netflix.spinnaker.keel.api.titus.TitusServerGroupSpec
import com.netflix.spinnaker.keel.api.toSimpleLocations
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.test.configuredTestYamlMapper
import com.netflix.spinnaker.keel.titus.resolve
import com.netflix.spinnaker.keel.titus.resolvePlatformSidecars
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import strikt.api.expectThat
import strikt.assertions.hasSize
import strikt.assertions.isA
import strikt.assertions.isEqualTo

class TitusClusterSpecDeserializationTests : JUnit5Minutests {

  data class Fixture(val manifest: String) {
    val mapper = configuredTestYamlMapper()
      .registerKeelTitusApiModule()
      .apply {
        registerSubtypes(NamedType(TitusClusterSpec::class.java, "titus/cluster@v1"))
      }
  }

  fun tests() = rootContext<Fixture> {
    context("a titus cluster where the locations are derived from the environment") {
      fixture {
        Fixture(
          """
               |---
               |application: fnord
               |serviceAccount: fzlem@spinnaker.io
               |environments:
               |  - name: test
               |    locations:
               |      account: test
               |      regions:
               |      - name: us-west-2
               |      - name: us-east-1
               |    resources:
               |    - kind: titus/cluster@v1
               |      spec:
               |        moniker:
               |          app: fnord
               |        container:
               |          organization: fnord
               |          image: fnord
               |          digest: sha:9e860d779528ea32b1692cdbb840c66c5d173b2c63aee0e7a75a957e06790de7
               |        resources:
               |          memory: 2048
               |          cpu: 4
               |      """.trimMargin())
      }

      test("locations on the cluster are set based on the environment") {
        val deliveryConfig = mapper.readValue<SubmittedDeliveryConfig>(manifest)

        expectThat(deliveryConfig.environments.first().resources.first().spec)
          .isA<TitusClusterSpec>()
          .and {
            get(TitusClusterSpec::locations).isEqualTo(deliveryConfig.environments.first().locations!!.toSimpleLocations())
            get(TitusClusterSpec::defaults).get { resources }.isEqualTo(ResourcesSpec(memory = 2048, cpu = 4))
          }

      }
    }

    context("with a job with platform sidecars") {
      fixture {
        Fixture(
          """
               |---
               |application: fnord
               |serviceAccount: fzlem@spinnaker.io
               |environments:
               |  - name: test
               |    locations:
               |      account: test
               |      regions:
               |      - name: us-west-2
               |    resources:
               |    - kind: titus/cluster@v1
               |      spec:
               |        moniker:
               |          app: fnord
               |        container:
               |          organization: fnord
               |          image: fnord
               |          digest: sha:9e860d779528ea32b1692cdbb840c66c5d173b2c63aee0e7a75a957e06790de7
               |        platformSidecars:
               |        - name: ps1
               |          channel: ps1channel
               |          arguments:
               |            foo: bar
               |        - name: ps2noargs
               |          channel: ps2channel
               |      """.trimMargin()
        )
      }

      test("the platform sidecars and arguments are correctly understood") {
        val deliveryConfig = mapper.readValue<SubmittedDeliveryConfig>(manifest)

        val expectedPlatformSidecars: List<TitusServerGroup.PlatformSidecar> = listOf(
          TitusServerGroup.PlatformSidecar("ps1", "ps1channel", arguments = mapOf("foo" to "bar")),
          TitusServerGroup.PlatformSidecar("ps2noargs", "ps2channel")
        )

        val spec = deliveryConfig.environments.first().resources.first().spec

        expectThat(spec)
          .isA<TitusClusterSpec>()

        val resolvedSpecSet = (spec as TitusClusterSpec).resolve()
        expectThat(resolvedSpecSet)
          .hasSize(1)

        val resolvedSpec = resolvedSpecSet.first()
        expectThat(resolvedSpec.platformSidecars)
          .isEqualTo(expectedPlatformSidecars)
      }

      test("platform sidecars work when overrides are present") {
        val deliveryConfig = mapper.readValue<SubmittedDeliveryConfig>(manifest)

        val expectedPlatformSidecars: List<TitusServerGroup.PlatformSidecar> = listOf(
          TitusServerGroup.PlatformSidecar("ps1", "ps1channel", arguments = mapOf("foo" to "bar")),
          TitusServerGroup.PlatformSidecar("ps2noargs", "ps2channel")
        )

        val spec = deliveryConfig.environments.first().resources.first().spec as TitusClusterSpec
        val specWithOverrides = spec.copy(overrides = mapOf("us-west-2" to TitusServerGroupSpec(containerAttributes= mapOf("foo" to "bar"))))

        val resolvedSpecSet = specWithOverrides.resolve()
        expectThat(resolvedSpecSet)
          .hasSize(1)

        val resolvedSpec = resolvedSpecSet.first()

        expectThat(resolvedSpec.platformSidecars)
          .isEqualTo(expectedPlatformSidecars)

      }
    }
  }
}
