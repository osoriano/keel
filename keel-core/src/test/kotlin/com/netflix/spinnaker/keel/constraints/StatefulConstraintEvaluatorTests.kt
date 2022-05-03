package com.netflix.spinnaker.keel.constraints

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.NotificationConfig
import com.netflix.spinnaker.keel.api.NotificationFrequency
import com.netflix.spinnaker.keel.api.NotificationType
import com.netflix.spinnaker.keel.api.StatefulConstraint
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.artifacts.fromBranch
import com.netflix.spinnaker.keel.api.constraints.ConstraintRepository
import com.netflix.spinnaker.keel.api.constraints.ConstraintState
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.api.constraints.DefaultConstraintAttributes
import com.netflix.spinnaker.keel.api.constraints.StatefulConstraintEvaluator
import com.netflix.spinnaker.keel.api.constraints.SupportedConstraintAttributesType
import com.netflix.spinnaker.keel.api.constraints.SupportedConstraintType
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.test.debianArtifact
import com.netflix.spinnaker.keel.test.resource
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.Runs
import io.mockk.coEvery as every
import io.mockk.coVerify as verify
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import java.time.Instant
import kotlinx.coroutines.runBlocking
import strikt.api.expectThat
import strikt.assertions.isEqualTo

internal class StatefulConstraintEvaluatorTests : JUnit5Minutests {

  class Fixture {
    val repository: ConstraintRepository = mockk(relaxUnitFun = true)
    val eventPublisher: EventPublisher = mockk(relaxed = true)
    val fakeStatefulConstraintEvaluatorDelegate: StatefulConstraintEvaluator<FakeConstraint, DefaultConstraintAttributes> = mockk(relaxed = true)

    class FakeConstraint : StatefulConstraint("fake")

    class FakeStatefulConstraintEvaluator(
      override val repository: ConstraintRepository,
      override val eventPublisher: EventPublisher,
      val delegate: StatefulConstraintEvaluator<FakeConstraint, DefaultConstraintAttributes>
    ) : StatefulConstraintEvaluator<FakeConstraint, DefaultConstraintAttributes> {
      override suspend fun constraintPasses(
        artifact: DeliveryArtifact,
        version: String,
        deliveryConfig: DeliveryConfig,
        targetEnvironment: Environment,
        constraint: FakeConstraint,
        state: ConstraintState
      ) =
        delegate.constraintPasses(artifact, version, deliveryConfig, targetEnvironment, constraint, state)

      override val supportedType = SupportedConstraintType<FakeConstraint>("fake")
      override val attributeType = SupportedConstraintAttributesType<DefaultConstraintAttributes>("fake")
    }

    val artifact = debianArtifact()

    val constraint = FakeConstraint()

    val environment = Environment(
      name = "test",
      notifications = setOf(
        NotificationConfig(
          type = NotificationType.slack,
          address = "#test",
          frequency = NotificationFrequency.normal
        )
      ),
      resources = setOf(resource()),
      constraints = setOf(constraint)
    )

    val manifest = DeliveryConfig(
      name = "test",
      application = "fnord",
      artifacts = setOf(artifact),
      environments = setOf(environment),
      serviceAccount = "keel@spinnaker"
    )

    val pendingConstraintState = ConstraintState(
      deliveryConfigName = "test",
      environmentName = "test",
      artifactVersion = "v1.0.0",
      artifactReference = artifact.reference,
      type = constraint.type,
      status = ConstraintStatus.PENDING
    )

    val subject = FakeStatefulConstraintEvaluator(
      repository, eventPublisher, fakeStatefulConstraintEvaluatorDelegate
    )
  }

  fun tests() = rootContext<Fixture> {
    fixture {
      Fixture()
    }

    before {
      every {
        eventPublisher.publishEvent(any())
      } just Runs

      every {
        repository.getConstraintState("test", "test", "v1.0.0", "fake", artifact.reference)
      } returns null

      every {
        repository.getConstraintState("test", "test", "v1.0.1", "fake", artifact.reference)
      } returns pendingConstraintState

      every {
        fakeStatefulConstraintEvaluatorDelegate.constraintPasses(artifact, "v1.0.0", manifest, environment, constraint, any())
      } returns true

    }

    test("abstract canPromote delegates to concrete sub-class") {
      // The method defined in StatefulConstraintEvaluator...
      runBlocking { subject.constraintPasses(artifact, "v1.0.0", manifest, environment) }

      val state = slot<ConstraintState>()
      verify {
        // ...in turns calls the constraintPasses method on the sub-class
        subject.constraintPasses(artifact, "v1.0.0", manifest, environment, constraint, capture(state))
      }
      // We ignore the timestamp because it's generated dynamically
      expectThat(state.captured).isEqualTo(pendingConstraintState.createdAt(state.captured.createdAt))
    }
  }

  private fun ConstraintState.createdAt(time: Instant) =
    copy(
      createdAt = time
    )
}
