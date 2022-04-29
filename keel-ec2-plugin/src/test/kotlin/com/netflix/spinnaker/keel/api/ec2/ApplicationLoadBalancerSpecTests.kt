package com.netflix.spinnaker.keel.api.ec2

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.keel.serialization.configuredYamlMapper
import dev.minutest.experimental.SKIP
import dev.minutest.experimental.minus
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isFalse
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import strikt.assertions.isTrue

internal object ApplicationLoadBalancerSpecTests : JUnit5Minutests {

  data class Fixture(
    val mapper: ObjectMapper = configuredYamlMapper(),
    val yaml: String
  )

  fun tests() = rootContext<Fixture> {
    context("a simple ALB definition in yaml") {
      fixture {
        Fixture(
          yaml =
          """
            |---
            |moniker:
            |  app: testapp
            |  stack: managedogge
            |  detail: wow
            |locations:
            |  account: test
            |  vpc: vpc0
            |  subnet: internal (vpc0)
            |  regions:
            |  - name: us-east-1
            |    availabilityZones:
            |    - us-east-1c
            |    - us-east-1d
            |    - us-east-1e
            |listeners:
            | - port: 80
            |   protocol: HTTP
            |targetGroups:
            | - name: managedogge-wow-tg
            |   port: 7001
          """.trimMargin()
        )
      }

      derivedContext<ApplicationLoadBalancerSpec>("when deserialized") {
        deriveFixture {
          mapper.readValue(yaml)
        }

        test("can be deserialized to an ALB object") {
          expectThat(this)
            .get { listeners.first().port }.isEqualTo(80)
        }

        test("populates default values for missing fields") {
          expectThat(this.targetGroups.first()) {
            get { healthCheckPath }.isEqualTo("/healthcheck")
            get { attributes.stickinessEnabled }.isFalse()
            get { attributes.stickinessType }.isNull()
            get { attributes.stickinessDuration }.isNull()
          }
        }
      }
    }

    context("an ALB definition with a sticky target group") {
      fixture {
        Fixture(
          yaml =
          """
            |---
            |moniker:
            |  app: testapp
            |  stack: managedogge
            |  detail: wow
            |locations:
            |  account: test
            |  vpc: vpc0
            |  subnet: internal (vpc0)
            |  regions:
            |  - name: us-east-1
            |listeners:
            | - port: 80
            |   protocol: HTTP
            |targetGroups:
            | - name: managedogge-wow-tg
            |   port: 7001
            |   attributes:
            |     stickinessEnabled: true
          """.trimMargin()
        )
      }

      derivedContext<ApplicationLoadBalancerSpec>("when deserialized") {
        deriveFixture {
          mapper.readValue(yaml)
        }

        test("populates default values for missing fields") {
          expectThat(this.targetGroups.first()) {
            get { attributes.stickinessEnabled }.isTrue()
            get { attributes.stickinessType }.isNotNull()
            get { attributes.stickinessDuration }.isNotNull()
          }
        }
      }
    }

    context("an ALB definition with misconfigured stickiness") {
      fixture {
        Fixture(
          yaml =
          """
              |---
              |moniker:
              |  app: testapp
              |  stack: managedogge
              |  detail: wow
              |locations:
              |  account: test
              |  vpc: vpc0
              |  subnet: internal (vpc0)
              |  regions:
              |  - name: us-east-1
              |listeners:
              | - port: 80
              |   protocol: HTTP
              |targetGroups:
              | - name: managedogge-wow-tg
              |   port: 7001
              |   attributes:
              |     stickinessEnabled: false
              |     stickinessDuration: 1200
            """.trimMargin()
        )
      }

      // TODO: reinstate if/when we add back the stickiness properties check in the model
      SKIP - test("fails to deserialize") {
        expectCatching<ApplicationLoadBalancerSpec> {
          mapper.readValue(yaml)
        }.isFailure()
          .isA<JsonMappingException>()
      }
    }
  }
}
