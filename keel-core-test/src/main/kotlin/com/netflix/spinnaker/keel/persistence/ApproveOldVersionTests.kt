package com.netflix.spinnaker.keel.persistence

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.netflix.spinnaker.config.ArtifactConfig
import com.netflix.spinnaker.keel.actuation.EnvironmentConstraintRunner
import com.netflix.spinnaker.keel.actuation.EnvironmentPromotionChecker
import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.artifacts.fromBranch
import com.netflix.spinnaker.keel.api.constraints.ConstraintState
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.OVERRIDE_PASS
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.PENDING
import com.netflix.spinnaker.keel.api.constraints.SupportedConstraintType
import com.netflix.spinnaker.keel.api.plugins.ApprovalConstraintEvaluator
import com.netflix.spinnaker.keel.api.plugins.ConstraintType.APPROVAL
import com.netflix.spinnaker.keel.api.plugins.kind
import com.netflix.spinnaker.keel.api.support.ConstraintRepositoryBridge
import com.netflix.spinnaker.keel.api.support.SpringEventPublisherBridge
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.constraints.ConstraintEvaluators
import com.netflix.spinnaker.keel.constraints.ManualJudgementConstraintEvaluator
import com.netflix.spinnaker.keel.core.api.DependsOnConstraint
import com.netflix.spinnaker.keel.core.api.ManualJudgementConstraint
import com.netflix.spinnaker.keel.resources.ResourceFactory
import com.netflix.spinnaker.keel.resources.ResourceSpecIdentifier
import com.netflix.spinnaker.keel.test.DummyArtifactReferenceResourceSpec
import com.netflix.spinnaker.keel.test.DummyResourceSpec
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import com.netflix.spinnaker.keel.test.resource
import com.netflix.spinnaker.keel.test.resourceFactory
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.springframework.context.ApplicationEventPublisher
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNull
import java.time.Instant
import io.mockk.coEvery as every

abstract class ApproveOldVersionTests<T : KeelRepository> : JUnit5Minutests {

  abstract fun createKeelRepository(resourceFactory: ResourceFactory, mapper: ObjectMapper): T

  open fun flush() {}

  class DummyImplicitConstraint : Constraint("implicit")

  class Fixture<T : KeelRepository>(
    val repositoryProvider: (ResourceFactory, ObjectMapper) -> T
  ) {

    val mapper: ObjectMapper = configuredTestObjectMapper().apply {
      registerSubtypes(NamedType(ManualJudgementConstraint::class.java, "manual-judgement"))
    }

    private val resourceSpecIdentifier: ResourceSpecIdentifier =
      ResourceSpecIdentifier(
        kind<DummyResourceSpec>("ec2/security-group@v1"),
        kind<DummyResourceSpec>("ec2/cluster@v1")
      )

    private val resourceFactory = resourceFactory(resourceSpecIdentifier)

    internal val repository = repositoryProvider(resourceFactory, mapper)

    val environment: Environment = Environment(
      name = "test",
      constraints = setOf(ManualJudgementConstraint()),
      resources = setOf(
        resource(
          spec = DummyArtifactReferenceResourceSpec(artifactReference = "my-artifact")
        )
      )
    )

    val publisher = mockk<ApplicationEventPublisher>(relaxUnitFun = true)
    val statelessEvaluator = mockk<ApprovalConstraintEvaluator<*>> {
      every { supportedType } returns SupportedConstraintType<DependsOnConstraint>("depends-on")
      every { isImplicit() } returns false
      every { isStateful() } returns false
      every { constraintType() } returns APPROVAL
    }
    val statefulEvaluator = ManualJudgementConstraintEvaluator(
      ConstraintRepositoryBridge(repository),
      MutableClock(),
      SpringEventPublisherBridge(publisher)
    )

    val implicitStatelessEvaluator = mockk<ApprovalConstraintEvaluator<DummyImplicitConstraint>> {
      every { supportedType } returns SupportedConstraintType("implicit")
      every { isImplicit() } returns true
      every { constraintPasses(any(), any(), any(), any()) } returns true
      every { isStateful() } returns false
      every { constraintType() } returns APPROVAL
    }
    val environmentConstraintRunner = EnvironmentConstraintRunner(
      repository,
      ConstraintEvaluators(listOf(statelessEvaluator, statefulEvaluator, implicitStatelessEvaluator))
    )

    val subject = EnvironmentPromotionChecker(
      repository,
      environmentConstraintRunner,
      publisher,
      ArtifactConfig(),
      MutableClock()
    )

    val artifact = DebianArtifact(
      name = "fnord",
      deliveryConfigName = "my-manifest",
      reference = "my-artifact",
      vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2")),
      from = fromBranch("main")
    )
    val deliveryConfig = DeliveryConfig(
      name = "my-manifest",
      application = "fnord",
      serviceAccount = "keel@spinnaker",
      environments = setOf(environment),
      artifacts = setOf(artifact)
    )

    val version1 = "fnord-0.0.1~dev.93-h93.3333333"
    val version2 = "fnord-0.0.1~dev.94-h94.4444444"

    val pendingManualJudgement1 = ConstraintState(
      deliveryConfig.name,
      environment.name,
      version1,
      artifact.reference,
      "manual-judgement",
      PENDING
    )

    val pendingManualJudgement2 = ConstraintState(
      deliveryConfig.name,
      environment.name,
      version2,
      artifact.reference,
      "manual-judgement",
      PENDING
    )

    val passedManualJudgement1 = ConstraintState(
      deliveryConfig.name,
      environment.name,
      version1,
      artifact.reference,
      "manual-judgement",
      OVERRIDE_PASS
    )

    val gitMetadata = GitMetadata(commit = "sha", branch = "main")
  }

  fun tests() = rootContext<Fixture<T>> {
    fixture {
      Fixture(
        repositoryProvider = this@ApproveOldVersionTests::createKeelRepository
      )
    }

    before {
      environmentConstraintRunner.onReady()
    }

    after {
      flush()
    }

    context("two versions of an artifact exist") {
      before {
        repository.register(artifact)
        repository.storeDeliveryConfig(deliveryConfig)
        repository.storeArtifactVersion(artifact.toArtifactVersion(version1, gitMetadata = gitMetadata, createdAt = Instant.now()))
        repository.storeArtifactVersion(artifact.toArtifactVersion(version2, gitMetadata = gitMetadata, createdAt = Instant.now()))
        repository.storeConstraintState(pendingManualJudgement1)
        repository.storeConstraintState(pendingManualJudgement2)

        every { statelessEvaluator.constraintPasses(artifact, version2, deliveryConfig, environment) } returns true
        every { statelessEvaluator.constraintPasses(artifact, version1, deliveryConfig, environment) } returns true
      }

      test("no version is approved, so the latest approved version is null") {
        runBlocking {
          subject.checkEnvironments(deliveryConfig)
        }

        expectThat(repository.latestVersionApprovedIn(deliveryConfig, artifact, environment.name)).isNull()
      }

      context("old version is approved") {
        before {
          repository.storeConstraintState(passedManualJudgement1)
        }

        test("so it is the latest approved version") {
          runBlocking {
            subject.checkEnvironments(deliveryConfig)
          }

          expectThat(repository.getConstraintState(deliveryConfig.name, environment.name, version1, "manual-judgement", artifact.reference))
            .get { this?.status }.isEqualTo(passedManualJudgement1.status)
          expectThat(repository.latestVersionApprovedIn(deliveryConfig, artifact, environment.name)).isEqualTo(version1)
        }
      }
    }
  }
}
