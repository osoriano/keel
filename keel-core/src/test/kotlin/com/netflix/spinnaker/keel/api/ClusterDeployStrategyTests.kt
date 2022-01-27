package com.netflix.spinnaker.keel.api

import com.fasterxml.jackson.databind.node.ObjectNode
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.jackson.at
import strikt.jackson.booleanValue
import strikt.jackson.hasSize
import strikt.jackson.isArray
import strikt.jackson.isBoolean
import strikt.jackson.isMissing
import strikt.jackson.isTextual
import strikt.jackson.numberValue
import strikt.jackson.path
import strikt.jackson.textValue

internal class ClusterDeployStrategyTests : JUnit5Minutests {
  data class Fixture(
    val strategy: ClusterDeployStrategy
  ) {
    val mapper = configuredObjectMapper()
  }

  fun tests() = rootContext<Fixture> {
    context("highlander") {
      fixture { Fixture(Highlander()) }

      test("serializes to JSON") {
        expectThat<ObjectNode>(mapper.valueToTree(strategy)) {
          path("strategy").textValue() isEqualTo "highlander"
          path("health").textValue() isEqualTo DeployHealth.AUTO.name
        }
      }
    }

    context("red-black") {
      fixture { Fixture(RedBlack()) }

      test("serializes to JSON") {
        expectThat<ObjectNode>(mapper.valueToTree(strategy)) {
          path("strategy").textValue() isEqualTo "red-black"
          path("health").textValue() isEqualTo DeployHealth.AUTO.name
          path("resizePreviousToZero").isBoolean().booleanValue().isFalse()
          path("rollbackOnFailure").isBoolean().booleanValue().isFalse()
          path("maxServerGroups").numberValue().isEqualTo(2)
          path("delayBeforeDisable").isTextual().textValue() isEqualTo "PT0S"
          path("delayBeforeScaleDown").isTextual().textValue() isEqualTo "PT0S"
          path("stagger").isMissing()
        }
      }
    }
  }
}
