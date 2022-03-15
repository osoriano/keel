package com.netflix.spinnaker.keel.actuation

import com.netflix.spinnaker.keel.actuation.EnvironmentConstraintRunner.EnvironmentContext
import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.SEMVER_TAG
import com.netflix.spinnaker.keel.api.constraints.ConstraintState
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.api.constraints.SupportedConstraintType
import com.netflix.spinnaker.keel.api.plugins.ApprovalConstraintEvaluator
import com.netflix.spinnaker.keel.api.plugins.ConstraintType.APPROVAL
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.core.api.DependsOnConstraint
import com.netflix.spinnaker.keel.core.api.ManualJudgementConstraint
import com.netflix.spinnaker.keel.core.api.TimeWindowConstraint
import com.netflix.spinnaker.keel.persistence.KeelRepository
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.coEvery as every
import io.mockk.mockk
import io.mockk.coVerify as verify
import kotlinx.coroutines.runBlocking
import strikt.api.expectCatching
import strikt.assertions.isSuccess

internal class EnvironmentConstraintRunnerTests : JUnit5Minutests {
  class DummyImplicitConstraint : Constraint("implicit")

  data class Fixture(
    val environment: Environment = Environment(
      name = "test"
    )
  ) {
    val repository: KeelRepository = mockk(relaxUnitFun = true)

    val statelessEvaluator = mockk<ApprovalConstraintEvaluator<*>> {
      every { supportedType } returns SupportedConstraintType<DependsOnConstraint>("depends-on")
      every { isImplicit() } returns false
      every { isStateful() } returns false
      every { constraintType() } returns APPROVAL
    }
    val mjEvaluator = mockk<ApprovalConstraintEvaluator<*>> {
      every { supportedType } returns SupportedConstraintType<ManualJudgementConstraint>("manual-judegment")
      every { isImplicit() } returns false
      every { isStateful() } returns true
      every { constraintType() } returns APPROVAL
    }

    val allowedTimesEvaluator = mockk<ApprovalConstraintEvaluator<*>> {
      every { supportedType } returns SupportedConstraintType<TimeWindowConstraint>("allowed-times")
      every { isImplicit() } returns false
      every { isStateful() } returns true
      every { constraintType() } returns APPROVAL
    }
    val implicitStatelessEvaluator = mockk<ApprovalConstraintEvaluator<*>> {
      every { supportedType } returns SupportedConstraintType<DummyImplicitConstraint>("implicit")
      every { isImplicit() } returns true
      every { constraintPasses(any(), any(), any(), any()) } returns true
      every { isStateful() } returns false
      every { constraintType() } returns APPROVAL
    }
    val subject = EnvironmentConstraintRunner(
      repository,
      listOf(statelessEvaluator, mjEvaluator, implicitStatelessEvaluator, allowedTimesEvaluator)
    )

    val artifact = DockerArtifact(
      name = "fnord",
      deliveryConfigName = "my-manifest",
      tagVersionStrategy = SEMVER_TAG
    )

    val deliveryConfig = DeliveryConfig(
      name = "my-manifest",
      application = "fnord",
      serviceAccount = "keel@spinnaker",
      environments = setOf(environment),
      artifacts = setOf(artifact)
    )

    val pendingManualJudgement = ConstraintState(
      deliveryConfig.name,
      environment.name,
      "2.0",
      artifact.reference,
      "manual-judgement",
      ConstraintStatus.PENDING
    )

    val passedManualJudgement = ConstraintState(
      deliveryConfig.name,
      environment.name,
      "1.2",
      artifact.reference,
      "manual-judgement",
      ConstraintStatus.OVERRIDE_PASS
    )

    fun generateContext(
      versions: List<String>,
      vetoedVersions: Set<String> = emptySet()
    ) =
      EnvironmentContext(deliveryConfig, environment, artifact, versions, vetoedVersions)
  }

  fun tests() = rootContext<Fixture> {
    fixture {
      Fixture()
    }

    context("no versions of an artifact exist") {
      test("the check does not throw an exception") {
        expectCatching {
          subject.checkEnvironment(generateContext(versions = emptyList()))
        }
          .isSuccess()
      }
    }

    context("versions exist") {
      before {
        every {
          repository.latestVersionApprovedIn(deliveryConfig, artifact, environment.name)
        } returns "1.2"

        every {
          repository.approveVersionFor(deliveryConfig, artifact, "2.0", environment.name)
        } returns true

        every {
          repository.getPendingConstraintsForArtifactVersions(any(), any(), any())
        } returns emptyList()
      }

      context("there are no constraints on the environment") {
        before {
          runBlocking {
            subject.checkEnvironment(generateContext(versions = listOf("2.0", "1.2", "1.1", "1.0")))
          }
        }

        test("the implicit constraint is checked") {
          verify {
            implicitStatelessEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment)
          }
        }

        test("the latest version is queued for approval") {
          verify {
            repository.queueArtifactVersionForApproval(deliveryConfig.name, environment.name, artifact, "2.0")
          }
        }
      }

      context("a version is vetoed") {
        before {
          runBlocking {
            subject.checkEnvironment(
              generateContext(
                versions = listOf("2.0", "1.2", "1.1", "1.0"),
                vetoedVersions = mutableSetOf("2.0")
              )
            )
          }
        }

        test("the version doesn't get checked") {
          verify(exactly = 0) {
            implicitStatelessEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment)
          }
        }
      }

      context("the latest version of the artifact was already approved for this environment") {
        before {

          every {
            repository.latestVersionApprovedIn(deliveryConfig, artifact, environment.name)
          } returns "2.0"

          runBlocking {
            subject.checkEnvironment(generateContext(versions = listOf("2.0", "1.2", "1.1", "1.0")))
          }
        }

        test("we don't re-queue the version") {
          verify(exactly = 0) {
            repository.queueArtifactVersionForApproval(deliveryConfig.name, environment.name, any(), any())
          }
        }
      }

      context("the environment has a simple constraint and a version can be found") {
        deriveFixture {
          copy(
            environment = Environment(
              name = "staging",
              constraints = setOf(DependsOnConstraint("test"))
            )
          )
        }
        before {
          // TODO: sucks that this is necessary but when using deriveFixture you get a different mockk
          every {
            repository.getPendingConstraintsForArtifactVersions(any(), any(), any())
          } returns emptyList()

          every { statelessEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment) } returns true
          every { mjEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment) } returns true
          every { statelessEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment) } returns true
          every { mjEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment) } returns true

          every {
            repository.latestVersionApprovedIn(deliveryConfig, artifact, environment.name)
          } returns "1.1"
        }

        context("a version is vetoed") {
          before {
            runBlocking {
              subject.checkEnvironment(
                generateContext(
                  versions = listOf("2.0", "1.2", "1.1", "1.0"),
                  vetoedVersions = mutableSetOf("2.0")
                )
              )
            }
          }

          test("the vetoed version doesn't get checked") {
            verify(exactly = 0) {
              implicitStatelessEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment)
            }
          }
          test("the other version is checked and queued for approval") {
            verify(exactly = 1) {
              implicitStatelessEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment)
              statelessEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment)
            }
          }

          test("the correct version is the only one queued for approval") {
            verify(exactly = 0) {
              repository.queueArtifactVersionForApproval(deliveryConfig.name, environment.name, artifact, "2.0")
            }
            verify(exactly = 1) {
              repository.queueArtifactVersionForApproval(deliveryConfig.name, environment.name, artifact, "1.2")
            }
          }
        }

        context("no versions are vetoed") {
          before {
            runBlocking {
              subject.checkEnvironment(
                generateContext(
                  versions = listOf("2.0", "1.2", "1.1", "1.0")
                )
              )
            }
          }

          test("we only check the latest version") {
            verify(exactly = 1) {
              implicitStatelessEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment)
              statelessEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment)
            }
            verify(exactly = 0) {
              implicitStatelessEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment)
              statelessEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment)
            }
          }

          test("the latest version is queued for approval") {
            verify(exactly = 1) {
              repository.queueArtifactVersionForApproval(deliveryConfig.name, environment.name, artifact, "2.0")
            }
          }
        }
      }

      context("the environment has constraints and a version can be found") {
        deriveFixture {
          copy(
            environment = Environment(
              name = "staging",
              constraints = setOf(DependsOnConstraint("test"), ManualJudgementConstraint())
            )
          )
        }
        before {
          // TODO: sucks that this is necessary but when using deriveFixture you get a different mockk
          every {
            repository.getConstraintState(any(), any(), "2.0", "manual-judgement", any())
          } returns pendingManualJudgement

          every {
            repository.getConstraintState(any(), any(), "1.2", "manual-judgement", any())
          } returns passedManualJudgement

          every {
            repository.getPendingConstraintsForArtifactVersions(any(), any(), any())
          } returns listOf("2.0", "1.2")
            .map { PublishedArtifact(artifact.name, artifact.type, it) }

          every {
            repository.constraintStateFor("my-manifest", "staging", "1.2", artifact.reference)
          } returns listOf(passedManualJudgement)

          every {
            repository.constraintStateFor("my-manifest", "staging", "2.0", artifact.reference)
          } returns listOf(pendingManualJudgement)

          every { statelessEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment) } returns true
          every { mjEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment) } returns true
          every { statelessEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment) } returns false
          every { mjEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment) } returns false

          every {
            repository.latestVersionApprovedIn(deliveryConfig, artifact, environment.name)
          } returns "1.1"
        }

        context("no versions are vetoed") {
          before {
            runBlocking {
              subject.checkEnvironment(generateContext(versions = listOf("2.0", "1.2", "1.1", "1.0")))
            }
          }

          test("the implicit constraint is checked") {
            verify {
              implicitStatelessEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment)
            }
          }

          test("the latest version that passes constraints is queued for approval") {
            verify {
              repository.queueArtifactVersionForApproval(deliveryConfig.name, environment.name, artifact, "1.2")
            }

            /**
             * Verify that stateful constraints are not checked if a stateless constraint blocks promotion
             */
            verify(exactly = 0) {
              mjEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment)
            }
          }
        }
      }

      context("the environment has several constraints and a version can be found") {
        deriveFixture {
          copy(
            environment = Environment(
              name = "staging",
              constraints = setOf(DependsOnConstraint("test"), ManualJudgementConstraint(), TimeWindowConstraint(windows = emptyList()))
            )
          )
        }
        before {
          // TODO: sucks that this is necessary but when using deriveFixture you get a different mockk
          every {
            repository.getConstraintState(any(), any(), "1.2", "manual-judgement", any())
          } returns passedManualJudgement

          every {
            repository.getPendingConstraintsForArtifactVersions(any(), any(), any())
          } returns listOf("1.2")
            .map { PublishedArtifact(artifact.name, artifact.type, it) }

          every {
            repository.constraintStateFor("my-manifest", "staging", "1.2", artifact.reference)
          } returns listOf(passedManualJudgement)


          every { statelessEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment) } returns true
          every { mjEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment) } returns true
          every { allowedTimesEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment) } returns false

          every {
            repository.latestVersionApprovedIn(deliveryConfig, artifact, environment.name)
          } returns "1.1"

          runBlocking {
            subject.checkEnvironment(generateContext(versions = listOf("1.2")))
          }
        }

        test("all stateful constraints are checked even if one fails") {
          verify(exactly = 1) {
            mjEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment)
            allowedTimesEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment)
          }
        }
      }

      context("the environment has constraints and a version cannot be found") {
        deriveFixture {
          copy(
            environment = Environment(
              name = "staging",
              constraints = setOf(DependsOnConstraint("test"))
            )
          )
        }

        before {
          // TODO: sucks that this is necessary but when using deriveFixture you get a different mockk
          every { statelessEvaluator.constraintPasses(artifact, "1.0", deliveryConfig, environment) } returns false
        }

        test("no exception is thrown") {
          expectCatching {
            subject.checkEnvironment(generateContext(versions = listOf("1.0")))
          }
            .isSuccess()
        }

        test("no artifact is registered") {
          runBlocking {
            subject.checkEnvironment(generateContext(versions = listOf("1.0")))
          }

          verify(exactly = 0) {
            repository.queueArtifactVersionForApproval(deliveryConfig.name, environment.name, any(), any())
          }
        }
      }

      context("the environment has a stateful constraint and a version cannot be found") {
        deriveFixture {
          copy(
            environment = Environment(
              name = "staging",
              constraints = setOf(ManualJudgementConstraint())
            )
          )
        }

        before {
          // TODO: sucks that this is necessary but when using deriveFixture you get a different mockk
          every { repository.getPendingConstraintsForArtifactVersions(any(), any(), any()) } returns emptyList()

          every { mjEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment) } returns false

          every {
            repository.constraintStateFor("my-manifest", "staging", "2.0", artifact.reference)
          } returns listOf(pendingManualJudgement)

          every { repository.latestVersionApprovedIn(any(), any(), any()) } returns null

          runBlocking { subject.checkEnvironment(generateContext(versions = listOf("2.0", "1.2", "1.1"))) }
        }

        test("stateful constraints are only evaluated for the most recent version") {
          verify(exactly = 1) {
            mjEvaluator.constraintPasses(artifact, "2.0", deliveryConfig, environment)
          }
          verify(exactly = 0) {
            mjEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment)
            mjEvaluator.constraintPasses(artifact, "1.1", deliveryConfig, environment)
          }
        }

        test("no artifact is approved") {
          verify(exactly = 0) {
            repository.queueArtifactVersionForApproval(deliveryConfig.name, environment.name, any(), any())
          }
        }

        test("no exception is thrown") {
          expectCatching {
            subject.checkEnvironment(generateContext(versions = listOf("2.0", "1.2", "1.1")))
          }
            .isSuccess()
        }
      }

      context("a new artifact passes stateful constraints while older versions are pending") {
        deriveFixture {
          copy(
            environment = Environment(
              name = "staging",
              constraints = setOf(DependsOnConstraint("test"), ManualJudgementConstraint())
            )
          )
        }

        before {
          // TODO: sucks that this is necessary but when using deriveFixture you get a different mockk
          every { repository.latestVersionApprovedIn(deliveryConfig, artifact, environment.name) } returns "1.1"

          every {
            repository.getPendingConstraintsForArtifactVersions(any(), any(), any())
          } returns listOf("1.2", "1.1")
            .map { PublishedArtifact(artifact.name, artifact.type, it) }

          every {
            repository.getArtifactVersionsQueuedForApproval(any(), any(), artifact)
          } returns setOf("1.2", "1.0")
            .map { PublishedArtifact(artifact.name, artifact.type, it) }

          every { mjEvaluator.constraintPasses(any(), "2.0", any(), any()) } returns false
          every { mjEvaluator.constraintPasses(any(), "1.2", any(), any()) } returns true
          every { mjEvaluator.constraintPasses(any(), "1.1", any(), any()) } returns false
          every { statelessEvaluator.constraintPasses(any(), "2.0", any(), any()) } returns true
          every { statelessEvaluator.constraintPasses(any(), "1.2", any(), any()) } returns true
          every { statelessEvaluator.constraintPasses(any(), "1.1", any(), any()) } returns true
          every { statelessEvaluator.constraintPasses(any(), "1.0", any(), any()) } returns true

          every { repository.approveVersionFor(deliveryConfig, artifact, "1.2", environment.name) } returns true
          every { repository.approveVersionFor(deliveryConfig, artifact, "1.0", environment.name) } returns true

          every {
            repository.constraintStateFor("my-manifest", "staging", "2.0", artifact.reference)
          } returns listOf(pendingManualJudgement)

          runBlocking { subject.checkEnvironment(generateContext(versions = listOf("2.0", "1.2", "1.1", "1.0", "0.9"))) }
        }

        test("pending versions are checked and approved if passed") {
          verify(exactly = 1) {
            mjEvaluator.constraintPasses(artifact, "1.2", deliveryConfig, environment)
            mjEvaluator.constraintPasses(artifact, "1.1", deliveryConfig, environment)
          }

          verify(exactly = 1) {
            repository.queueArtifactVersionForApproval(deliveryConfig.name, environment.name, artifact, "1.2")
          }

          verify(exactly = 0) {
            repository.queueArtifactVersionForApproval(deliveryConfig.name, environment.name, artifact, "2.0")
          }
        }
      }
    }
  }
}
