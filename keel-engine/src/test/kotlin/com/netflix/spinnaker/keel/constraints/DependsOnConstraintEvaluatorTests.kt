package com.netflix.spinnaker.keel.constraints

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.api.action.ActionRepository
import com.netflix.spinnaker.keel.api.action.ActionType.VERIFICATION
import com.netflix.spinnaker.keel.api.artifacts.fromBranch
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.core.api.DependsOnConstraint
import com.netflix.spinnaker.keel.persistence.ArtifactRepository
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.test.debianArtifact
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isFalse
import strikt.assertions.isNotNull
import strikt.assertions.isTrue
import java.time.Duration

internal class DependsOnConstraintEvaluatorTests : JUnit5Minutests {

  object Fixture {
    val artifact = debianArtifact()
    val constrainedEnvironment = Environment(
      name = "staging",
      constraints = setOf(
        DependsOnConstraint(environment = "test")
      )
    )
    val previousEnvironment = Environment(
      name = "test"
    )
    val deliveryConfig = DeliveryConfig(
      name = "my-manifest",
      application = "fnord",
      serviceAccount = "keel@spinnaker",
      artifacts = setOf(artifact),
      environments = setOf(previousEnvironment, constrainedEnvironment)
    )
    val constrainedEnvironmentWithDelay = constrainedEnvironment.copy(
      constraints = setOf(
        DependsOnConstraint(environment = "test", deployAfter = Duration.ofMinutes(5))
      )
    )
    val deliveryConfigWithDelay = deliveryConfig.copy(
      environments = setOf(previousEnvironment, constrainedEnvironmentWithDelay)
    )

    val artifactRepository: ArtifactRepository = mockk(relaxUnitFun = true)
    val actionRepository: ActionRepository = mockk {
      every { allPassed(any(), any()) } returns true
      every { allStarted(any(), any()) } returns true
    }
    val clock = MutableClock()

    val subject = DependsOnConstraintEvaluator(artifactRepository, actionRepository, mockk(), clock)
  }

  fun tests() = rootContext<Fixture> {
    fixture { Fixture }

    before {
      every {
        actionRepository.getStates(any(), VERIFICATION)
      } returns emptyMap()
    }

    test("an invalid environment name causes an exception") {
      expectCatching {
        subject.constraintPasses(artifact, "1.1", deliveryConfig, Environment(name = "foo"))
      }
        .isFailure()
        .isA<IllegalArgumentException>()
    }

    test("an environment without the constraint throws an exception (don't pass it to this method)") {
      expectCatching { subject.constraintPasses(artifact, "1.1", deliveryConfig, previousEnvironment) }
        .isFailure()
    }

    test("constraint serializes with type information") {
      val mapper = configuredObjectMapper()
      val serialized = mapper.writeValueAsString(constrainedEnvironment.constraints)
      expectThat(serialized)
        .contains("depends-on")
    }

    context("the requested version is not in the required environment") {
      before {
        every {
          artifactRepository.wasSuccessfullyDeployedTo(any(), artifact, "1.1", previousEnvironment.name)
        } returns false
      }

      test("promotion is vetoed") {
        expectThat(runBlocking { subject.constraintPasses(artifact, "1.1", deliveryConfig, constrainedEnvironment)})
          .isFalse()
      }

      test("with deploy delay") {
        runBlocking {
          expectThat(
            subject.constraintPasses(artifact, "1.1", deliveryConfigWithDelay, constrainedEnvironmentWithDelay))
            .isFalse()
        }
      }
    }

    context("the requested version is in the required environment") {
      before {
        every {
          artifactRepository.wasSuccessfullyDeployedTo(any(), artifact, "1.1", previousEnvironment.name)
        } returns true

        every {
          artifactRepository.getDeployedAt(any(), previousEnvironment, artifact, "1.1")
        } returns clock.instant()
      }

      context("with no deploy delay specified") {
        test("promotion is allowed") {
          runBlocking {
            expectThat(subject.constraintPasses(artifact, "1.1", deliveryConfig, constrainedEnvironment))
              .isTrue()
          }
        }
      }

      context("with deploy delay that hasn't elapsed yet") {
        test("promotion is not allowed") {
          runBlocking {
            expectThat(
              subject.constraintPasses(artifact, "1.1", deliveryConfigWithDelay, constrainedEnvironmentWithDelay))
              .isFalse()
          }
        }
      }

      context("with deploy delay that has elapsed") {
        test("promotion is allowed") {
          clock.tickMinutes(6)
          runBlocking {
            expectThat(
              subject.constraintPasses(artifact, "1.1", deliveryConfigWithDelay, constrainedEnvironmentWithDelay))
              .isTrue()
          }
        }
      }
    }

    context("generating constraint state") {
      test("can get state") {
        val state = runBlocking { subject.generateConstraintStateSnapshot(artifact = artifact, version = "1.1", deliveryConfig = deliveryConfig, targetEnvironment = constrainedEnvironment) }
        expectThat(state)
          .and { get { type }.isEqualTo("depends-on") }
          .and { get { status }.isEqualTo(ConstraintStatus.PASS) }
          .and { get { judgedAt }.isNotNull() }
          .and { get { judgedBy }.isNotNull() }
          .and { get { attributes }.isNotNull() }
      }
    }
  }
}
