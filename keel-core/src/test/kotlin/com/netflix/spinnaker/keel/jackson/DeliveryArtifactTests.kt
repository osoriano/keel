package com.netflix.spinnaker.keel.jackson

import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.FROM_ANY_BRANCH
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.containsKey
import strikt.assertions.doesNotContainKey
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isSuccess

internal class DeliveryArtifactTests : JUnit5Minutests {
  private val debianArtifact =
    """
      {
        "name": "fnord",
        "type": "deb",
        "deliveryConfigName": "my-delivery-config",
        "vmOptions": {
          "baseLabel": "RELEASE",
          "baseOs": "bionic",
          "storeType": "EBS",
          "regions": [
            "ap-south-1"
          ]
        },
        "from": {
          "branch": {
            "name": "main"
          }
        }
      }
    """.trimIndent()

  private val debianArtifactWithoutBranchFilter =
    """
      {
        "name": "fnord",
        "type": "deb",
        "deliveryConfigName": "my-delivery-config",
        "vmOptions": {
          "baseLabel": "RELEASE",
          "baseOs": "bionic",
          "storeType": "EBS",
          "regions": [
            "ap-south-1"
          ]
        }
      }
    """.trimIndent()

  private val dockerArtifact =
    """
      {
        "name": "fnord/blah",
        "type": "docker",
        "deliveryConfigName": "my-delivery-config",
        "tagVersionStrategy": "semver-job-commit-by-job"
      }
    """.trimIndent()

  private val slashyDockerArtifact =
    """
      {
        "name": "fnord/is/cool",
        "type": "docker",
        "deliveryConfigName": "my-delivery-config",
        "tagVersionStrategy": "semver-job-commit-by-job"
      }
    """.trimIndent()

  private val mapper = configuredTestObjectMapper().apply {
    registerSubtypes(NamedType<DockerArtifact>(DOCKER))
    registerSubtypes(NamedType<DebianArtifact>(DEBIAN))
  }

  fun tests() = rootContext {
    context("docker name validation") {
      test("rejects slashy") {
        expectCatching { mapper.readValue<DeliveryArtifact>(slashyDockerArtifact) }
          .isFailure()
      }
    }

    mapOf(
      debianArtifact to DebianArtifact::class.java,
      dockerArtifact to DockerArtifact::class.java
    ).forEach { (json, type) ->

      context("deserialization of ${type.simpleName}") {
        test("works") {
          expectCatching { mapper.readValue<DeliveryArtifact>(json) }
            .isSuccess()
            .get { javaClass.name }.isEqualTo(type.name)
        }
      }

      derivedContext<Map<String, Any?>>("serialization of ${type.simpleName}") {
        deriveFixture {
          mapper.readValue<DeliveryArtifact>(json)
            .let { mapper.writeValueAsString(it) } // ensure serializing to JSON works
            .let { mapper.readValue(it) } // read back as a Map
        }

        test("ignores deliveryConfigName") {
          expectThat(this).doesNotContainKey(DeliveryArtifact::deliveryConfigName.name)
        }

        test("ignores sortingStrategy") {
          expectThat(this).doesNotContainKey(DeliveryArtifact::sortingStrategy.name)
        }
      }
    }

    context("deserialization of debian artifact without branch filter") {
      test("sets default branch filter") {
        expectCatching {
          mapper.readValue<DebianArtifact>(debianArtifactWithoutBranchFilter)
        }.isSuccess()
          .isA<DebianArtifact>()
          .get { from }.isEqualTo(FROM_ANY_BRANCH)
      }
    }
  }
}
