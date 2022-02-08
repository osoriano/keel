package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.keel.api.artifacts.ArtifactStatus
import com.netflix.spinnaker.keel.api.artifacts.BranchFilter
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.core.api.DependsOnConstraint
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.core.api.SubmittedEnvironment
import com.netflix.spinnaker.keel.core.api.SubmittedResource
import com.netflix.spinnaker.keel.test.DummyResourceSpec
import com.netflix.spinnaker.keel.test.TEST_API_V1
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import strikt.api.expectThat
import strikt.assertions.isFalse
import strikt.assertions.isTrue

class SubmittedDeliveryConfigTests : JUnit5Minutests {

  private val deliveryConfig = SubmittedDeliveryConfig(
    name = "keel-manifest",
    application = "keel",
    serviceAccount = "keel@spinnaker",
    artifacts = setOf(DebianArtifact(
      name = "keel",
      vmOptions = VirtualMachineOptions(
        baseOs = "bionic",
        regions = setOf("us-west-2")
      ),
      statuses = setOf(ArtifactStatus.RELEASE)
    ),
      DebianArtifact(
        name = "keel-master",
        vmOptions = VirtualMachineOptions(
          baseOs = "bionic",
          regions = setOf("us-west-2")
        ),
        branch = "master"
      )
    ),
    environments = setOf(
      SubmittedEnvironment(
        name = "test",
        resources = setOf(
          SubmittedResource(
            kind = TEST_API_V1.qualify("whatever"),
            spec = DummyResourceSpec(data = "resource in test")
          )
        )
      ),
      SubmittedEnvironment(
        name = "prod",
        resources = setOf(
          SubmittedResource(
            kind = TEST_API_V1.qualify("whatever"),
            spec = DummyResourceSpec(data = "resource in prod")
          )
        ),
        constraints = setOf(DependsOnConstraint("test"))
      )
    ),
    previewEnvironments = setOf(
      PreviewEnvironmentSpec(
        branch = BranchFilter(startsWith = "feature/"),
        baseEnvironment = "test"
      )
    )
  )

  private val deliveryConfigWithBranch = deliveryConfig.copy(
    artifacts = setOf(DebianArtifact(
      name = "keel",
      vmOptions = VirtualMachineOptions(
        baseOs = "bionic",
        regions = setOf("us-west-2")
      ),
      branch = "master"
    )
    ))

  fun tests() = rootContext<Unit> {
    context("artifact with statuses") {
      test("delivery config with an artifact containing branches return false") {
        expectThat(
          deliveryConfigWithBranch.artifactWithStatuses
        ).isFalse()
      }

      test("delivery config with an artifact containing at least one artifact with status return true") {
        expectThat(
          deliveryConfig.artifactWithStatuses
        ).isTrue()
      }
    }
  }
}
