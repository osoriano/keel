package com.netflix.spinnaker.keel.persistence

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.core.api.ActionMetadata
import com.netflix.spinnaker.keel.core.api.ArtifactVersionVetoData
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactPin
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVeto
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVetoes
import com.netflix.spinnaker.keel.core.api.PinnedEnvironment
import com.netflix.spinnaker.keel.core.api.PromotionStatus.CURRENT
import com.netflix.spinnaker.keel.core.api.PromotionStatus.DEPLOYING
import com.netflix.spinnaker.keel.core.api.PromotionStatus.PENDING
import com.netflix.spinnaker.keel.core.api.PromotionStatus.PREVIOUS
import com.netflix.spinnaker.keel.core.api.PromotionStatus.SKIPPED
import com.netflix.spinnaker.keel.persistence.ArtifactRepositoryTests.ArtifactRepositoryFixture
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.mockk
import org.springframework.context.ApplicationEventPublisher
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.doesNotContainKey
import strikt.assertions.getValue
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isNotEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import strikt.assertions.isSuccess
import strikt.assertions.isTrue
import java.time.Duration

/**
 * All the promotion flow tests around artifacts
 */
abstract class ArtifactRepositoryPromotionFlowTests<T : ArtifactRepository> : JUnit5Minutests {
  val publisher: ApplicationEventPublisher = mockk(relaxed = true)

  val clock = MutableClock()

  abstract fun factory(): T

  abstract fun deliveryConfigRepository(): DeliveryConfigRepository

  open fun T.flush() {}

  data class Fixture<T : ArtifactRepository>(
    override val subject: T,
    override val deliveryConfigRepository: DeliveryConfigRepository,
    override val clock: MutableClock
  ) : ArtifactRepositoryFixture<T>(subject, deliveryConfigRepository, clock) {
    val previewEnvironment1 = Environment("test-preview-branch1", isPreview = true)
      .addMetadata("branch" to "feature/preview-branch1")
    val previewEnvironment2 = Environment("test-preview-branch2", isPreview = true)
      .addMetadata("branch" to "feature/preview-branch2")
    override val manifest = DeliveryConfig(
      name = "my-manifest",
      application = "fnord",
      serviceAccount = "keel@spinnaker",
      artifacts = setOf(
        versionedDockerArtifact,
        debianFromMainBranch,
        debianFromBranchPrefix,
        debianFromBranchPattern,
        debianFromPullRequest,
        debianFromPullRequestAndBranch
      ),
      environments = setOf(testEnvironment, stagingEnvironment, previewEnvironment1, previewEnvironment2)
    )

    val dockerVersion1 = "v1.0.1-h6.322963d"
    val dockerVersion2 = "v1.0.2-h7.c53c9cf"

    override val dockerVersions = listOf(dockerVersion1, dockerVersion2)

    val debVersionFromPr1 = "keeldemo-pull.7-h20.d0349c3" // pull request build
    val debVersionFromPr2 = "keeldemo-pull.8-h21.27dc978" // pull request build

    val pin1 = EnvironmentArtifactPin(
      targetEnvironment = stagingEnvironment.name, // staging
      reference = debianFromMainBranch.reference,
      version = debVersion1,
      pinnedBy = "keel@spinnaker",
      comment = "fnord"
    )
  }

  fun tests() = rootContext<Fixture<T>> {
    fixture { Fixture(factory(), deliveryConfigRepository(), clock) }

    before {
      persist(storeVersions = true)
    }

    after {
      subject.flush()
    }

    context("no version has been promoted to an environment") {
      test("the approved version for that environment is null") {
        expectThat(subject.latestVersionApprovedIn(manifest, debianFromMainBranch, testEnvironment.name))
          .isNull()
      }

      test("versions are not considered successfully deployed") {
        setOf(debVersion1, debVersion2, debVersion3).forEach {
          expectThat(subject.wasSuccessfullyDeployedTo(manifest, debianFromMainBranch, it, testEnvironment.name))
            .isFalse()
        }
      }

      test("the artifact version is pending in the environment") {
        expectThat(versionsIn(testEnvironment, debianFromMainBranch)) {
          getValue(PENDING).containsExactlyInAnyOrder(debVersion1, debVersion2, debVersion3)
          doesNotContainKey(CURRENT)
          doesNotContainKey(DEPLOYING)
          doesNotContainKey(PREVIOUS)
        }

        expectThat(subject.isDeployingTo(manifest, testEnvironment.name)).isFalse()
      }

      context("pending versions") {
        test("can get pending versions with matching branch") {
          expectThat(
            subject.getPendingVersionsInEnvironment(
              manifest,
              debianFromMainBranch.reference,
              testEnvironment.name
            ).size
          ).isEqualTo(3)
        }

        test("can get versions with matching branch pattern") {
          storeVersionsForDebianFromBranchPattern((20 downTo 1).map { "keeldemo-any-string-$it" })
          expectThat(
            subject.getPendingVersionsInEnvironment(
              manifest,
              debianFromBranchPattern.reference,
              testEnvironment.name
            ).size
          ).isEqualTo(15)
        }

        test("can get versions from pull requests") {
          storeVersionsForDebianFromPullRequest((20 downTo 1).map { "keeldemo-any-string-$it" })
          expectThat(
            subject.getPendingVersionsInEnvironment(
              manifest,
              debianFromPullRequest.reference,
              testEnvironment.name
            ).size
          ).isEqualTo(10)
        }

        test("fetch only versions matching the preview environment branch") {
          storeArtifactVersionWithMetadata(debianFromBranchPrefix, debVersionFromPr1, previewEnvironment1.branch!!)
          storeArtifactVersionWithMetadata(debianFromBranchPrefix, debVersionFromPr2, previewEnvironment2.branch!!)

          val env1Versions = subject.getPendingVersionsInEnvironment(
            manifest,
            debianFromBranchPrefix.reference,
            previewEnvironment1.name
          )

          val env2Versions = subject.getPendingVersionsInEnvironment(
            manifest,
            debianFromBranchPrefix.reference,
            previewEnvironment2.name
          )

          expectThat(env1Versions.map { it.version })
            .containsExactly(debVersionFromPr1)

          expectThat(env2Versions.map { it.version })
            .containsExactly(debVersionFromPr2)

          expectThat(versionsIn(previewEnvironment1, debianFromBranchPrefix)) {
            getValue(PENDING).containsExactly(debVersionFromPr1)
          }

          expectThat(versionsIn(previewEnvironment2, debianFromBranchPrefix)) {
            getValue(PENDING).containsExactly(debVersionFromPr2)
          }
        }
      }

      test("an artifact version can be vetoed even if it was not previously deployed") {
        val veto = EnvironmentArtifactVeto(
          targetEnvironment = testEnvironment.name,
          reference = debianFromMainBranch.reference,
          version = debVersion1,
          vetoedBy = "someone",
          comment = "testing if mark as bad works"
        )

        subject.markAsVetoedIn(deliveryConfig = manifest, veto = veto, force = true)

        expectThat(
          subject.vetoedEnvironmentVersions(manifest).map {
            it.copy(versions = it.versions.map { v -> v.copy(vetoedAt = null) }.toMutableSet())
          }
        )
          .isEqualTo(
            listOf(
              EnvironmentArtifactVetoes(
                deliveryConfigName = manifest.name,
                targetEnvironment = testEnvironment.name,
                artifact = debianFromMainBranch,
                versions = mutableSetOf(
                  ArtifactVersionVetoData(
                    version = veto.version,
                    vetoedBy = veto.vetoedBy,
                    vetoedAt = null,
                    comment = veto.comment
                  )
                )
              )
            )
          )
      }
    }

    context("another version is stuck in deploying") {
      before {
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
      }

      test("we update the status of the old version when we mark the new one deploying") {
        val v1summary = subject.getArtifactSummaryInEnvironment(
          manifest,
          testEnvironment.name,
          debianFromMainBranch.reference,
          debVersion1
        )
        expectThat(v1summary)
          .isNotNull()
          .get { state }
          .isEqualTo("skipped")
      }
    }

    context("we mark old pending versions as skipped") {
      before {
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
      }

      test("we mark version 1 as skipped") {
        val artifactInEnvSummary =
          subject.getAllVersionsForEnvironment(debianFromMainBranch, manifest, testEnvironment.name)
        expectThat(artifactInEnvSummary.find { it.publishedArtifact.version == debVersion1 }?.status)
          .isNotNull()
          .isEqualTo(SKIPPED)
      }
    }

    context("a version has been promoted to an environment") {
      before {
        clock.incrementBy(Duration.ofHours(1))
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        subject.approveVersionFor(manifest, versionedDockerArtifact, dockerVersion1, stagingEnvironment.name)
        subject.markAsDeployingTo(manifest, versionedDockerArtifact, dockerVersion1, stagingEnvironment.name)
      }

      test("the approved version for that environment matches") {
        // debian
        expectThat(subject.latestVersionApprovedIn(manifest, debianFromMainBranch, testEnvironment.name))
          .isEqualTo(debVersion1)
        // docker
        expectThat(subject.latestVersionApprovedIn(manifest, versionedDockerArtifact, stagingEnvironment.name))
          .isEqualTo(dockerVersion1)
      }

      test("the version is not considered successfully deployed yet") {
        expectThat(subject.wasSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name))
          .isFalse()
        expectThat(
          subject.wasSuccessfullyDeployedTo(
            manifest,
            versionedDockerArtifact,
            dockerVersion1,
            stagingEnvironment.name
          )
        )
          .isFalse()
      }

      test("the version is deploying in the environment") {
        expectThat(versionsIn(testEnvironment, debianFromMainBranch)) {
          getValue(PENDING).containsExactlyInAnyOrder(debVersion2, debVersion3)
          getValue(DEPLOYING).containsExactly(debVersion1)
          doesNotContainKey(CURRENT)
          doesNotContainKey(PREVIOUS)
        }

        expectThat(subject.isDeployingTo(manifest, testEnvironment.name)).isTrue()

        expectThat(versionsIn(stagingEnvironment, versionedDockerArtifact)) {
          getValue(PENDING).containsExactlyInAnyOrder(dockerVersion2)
          getValue(DEPLOYING).containsExactly(dockerVersion1)
          doesNotContainKey(CURRENT)
          doesNotContainKey(PREVIOUS)
        }

        expectThat(subject.isDeployingTo(manifest, stagingEnvironment.name)).isTrue()
      }

      test("promoting the same version again returns false") {
        expectCatching {
          clock.incrementBy(Duration.ofHours(1))
          subject.approveVersionFor(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        }
          .isSuccess()
          .isFalse()
      }

      test("promoting a new version returns true") {
        expectCatching {
          clock.incrementBy(Duration.ofHours(1))
          subject.approveVersionFor(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        }
          .isSuccess()
          .isTrue()
      }

      context("the version is marked as successfully deployed") {
        before {
          subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
          subject.markAsSuccessfullyDeployedTo(manifest, versionedDockerArtifact, dockerVersion1, stagingEnvironment.name)
        }

        test("the version is now considered successfully deployed") {
          expectThat(
            subject.wasSuccessfullyDeployedTo(
              manifest,
              debianFromMainBranch,
              debVersion1,
              testEnvironment.name
            )
          )
            .isTrue()
          expectThat(
            subject.wasSuccessfullyDeployedTo(
              manifest,
              versionedDockerArtifact,
              dockerVersion1,
              stagingEnvironment.name
            )
          )
            .isTrue()
        }

        test("the version is marked as currently deployed") {
          expectThat(subject.isCurrentlyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name))
            .isTrue()
          expectThat(
            subject.isCurrentlyDeployedTo(
              manifest,
              versionedDockerArtifact,
              dockerVersion1,
              stagingEnvironment.name
            )
          )
            .isTrue()
        }

        test("the version is current in the environment") {
          expectThat(versionsIn(testEnvironment, debianFromMainBranch)) {
            getValue(PENDING).containsExactlyInAnyOrder(debVersion2, debVersion3)
            getValue(CURRENT).containsExactly(debVersion1)
            doesNotContainKey(DEPLOYING)
            doesNotContainKey(PREVIOUS)
          }

          expectThat(subject.isDeployingTo(manifest, testEnvironment.name)).isFalse()
        }

        test("the version is still current when it is re deployed (like if the base ami updates)") {
          subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
          val versions = subject.getAllVersionsForEnvironment(debianFromMainBranch, manifest, testEnvironment.name)
          expect {
            that(versions.size).isEqualTo(3)
            that(versions.map { it.status }).containsExactlyInAnyOrder(listOf(DEPLOYING, PENDING, PENDING))
            that(versions.first { it.status == DEPLOYING }.publishedArtifact.version).isEqualTo(debVersion1)
            that(versions.first { it.status == DEPLOYING }.isCurrent).isEqualTo(true)
          }
        }

        test("querying for current returns the full artifact") {
          val artifacts =
            subject.getArtifactVersionsByStatus(manifest, debianFromMainBranch.reference, testEnvironment.name, listOf(CURRENT))
          expect {
            that(artifacts.size).isEqualTo(1)
            that(artifacts.first().version).isEqualTo(debVersion1)
          }
          expectThat(
            subject.getCurrentlyDeployedArtifactVersion(
              manifest,
              debianFromMainBranch,
              testEnvironment.name
            )?.version
          ).isEqualTo(debVersion1)
        }

        context("a new version is promoted to the same environment") {
          before {
            clock.incrementBy(Duration.ofHours(1))
            subject.approveVersionFor(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
            subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
          }

          test("the latest approved version changes") {
            expectThat(subject.latestVersionApprovedIn(manifest, debianFromMainBranch, testEnvironment.name))
              .isEqualTo(debVersion2)
          }

          test("the version is not considered successfully deployed yet") {
            expectThat(
              subject.wasSuccessfullyDeployedTo(
                manifest,
                debianFromMainBranch,
                debVersion2,
                testEnvironment.name
              )
            )
              .isFalse()
          }

          test("the new version is deploying in the environment") {
            expectThat(versionsIn(testEnvironment, debianFromMainBranch)) {
              getValue(PENDING).containsExactly(debVersion3)
              getValue(CURRENT).containsExactly(debVersion1)
              getValue(DEPLOYING).containsExactly(debVersion2)
              doesNotContainKey(PREVIOUS)
            }

            expectThat(subject.isDeployingTo(manifest, testEnvironment.name)).isTrue()

          }

          context("the new version is marked as successfully deployed") {
            before {
              clock.incrementBy(Duration.ofMinutes(30))
              subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
            }

            test("the old version is still considered successfully deployed") {
              expectThat(
                subject.wasSuccessfullyDeployedTo(
                  manifest,
                  debianFromMainBranch,
                  debVersion1,
                  testEnvironment.name
                )
              )
                .isTrue()
            }

            test("the old version is not considered currently deployed") {
              expectThat(
                subject.isCurrentlyDeployedTo(
                  manifest,
                  debianFromMainBranch,
                  debVersion1,
                  testEnvironment.name
                )
              )
                .isFalse()
            }

            test("the new version is also considered successfully deployed") {
              expectThat(
                subject.wasSuccessfullyDeployedTo(
                  manifest,
                  debianFromMainBranch,
                  debVersion2,
                  testEnvironment.name
                )
              )
                .isTrue()
            }

            test("the new version is current in the environment") {
              expectThat(versionsIn(testEnvironment, debianFromMainBranch)) {
                getValue(PENDING).containsExactlyInAnyOrder(debVersion3)
                getValue(CURRENT).containsExactly(debVersion2)
                getValue(PREVIOUS).containsExactly(debVersion1)
                doesNotContainKey(DEPLOYING)
              }

              expectThat(subject.isDeployingTo(manifest, testEnvironment.name)).isFalse()

              expectThat(
                subject.getCurrentlyDeployedArtifactVersion(
                  manifest,
                  debianFromMainBranch,
                  testEnvironment.name
                )?.version
              ).isEqualTo(debVersion2)
            }
          }
        }

        context("there are two approved versions for the environment and the latter was deployed") {
          before {
            clock.incrementBy(Duration.ofHours(1))
            subject.approveVersionFor(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
            subject.approveVersionFor(manifest, debianFromMainBranch, debVersion3, testEnvironment.name)
            subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion3, testEnvironment.name)
          }

          test("the lower version was marked as skipped") {
            val result = versionsIn(testEnvironment, debianFromMainBranch)
            expectThat(result) {
              getValue(CURRENT).containsExactly(debVersion3)
              getValue(PREVIOUS).containsExactly(debVersion1)
              getValue(SKIPPED).containsExactly(debVersion2)
              doesNotContainKey(PENDING)
              doesNotContainKey(DEPLOYING)
            }

            expectThat(subject.isDeployingTo(manifest, testEnvironment.name)).isFalse()
          }

          test("can get all information about the versions") {
            val versions = subject.getAllVersionsForEnvironment(debianFromMainBranch, manifest, testEnvironment.name)
            expect {
              that(versions.size).isEqualTo(3)
              that(versions.map { it.status }).containsExactlyInAnyOrder(listOf(CURRENT, PREVIOUS, SKIPPED))
              that(versions.first { it.status == CURRENT }.publishedArtifact.version).isEqualTo(debVersion3)
              that(versions.first { it.status == PREVIOUS }.publishedArtifact.version).isEqualTo(debVersion1)
              that(versions.first { it.status == SKIPPED }.publishedArtifact.version).isEqualTo(debVersion2)
            }
          }
        }
      }

      context("a version of a different artifact is promoted to the environment") {
        before {
          clock.incrementBy(Duration.ofHours(1))
          subject.approveVersionFor(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        }

        test("the approved version of the original artifact remains the same") {
          expectThat(subject.latestVersionApprovedIn(manifest, debianFromMainBranch, testEnvironment.name))
            .isEqualTo(debVersion1)
        }

        test("the approved version of the new artifact matches") {
          expectThat(subject.latestVersionApprovedIn(manifest, debianFromMainBranch, testEnvironment.name))
            .isEqualTo(debVersion1)
        }
      }

      context("a different version of the same artifact is promoted to another environment") {
        before {
          clock.incrementBy(Duration.ofHours(1))
          subject.approveVersionFor(manifest, debianFromMainBranch, debVersion2, stagingEnvironment.name)
        }

        test("the approved version in the original environment is unaffected") {
          expectThat(subject.latestVersionApprovedIn(manifest, debianFromMainBranch, testEnvironment.name))
            .isEqualTo(debVersion1)
        }

        test("the approved version in the new environment matches") {
          expectThat(subject.latestVersionApprovedIn(manifest, debianFromMainBranch, stagingEnvironment.name))
            .isEqualTo(debVersion2)
        }
      }
    }

    context("there are skipped versions that are being evaluated") {
      before {
        clock.incrementBy(Duration.ofHours(1))
        subject.approveVersionFor(manifest, versionedDockerArtifact, dockerVersion2, testEnvironment.name)
        subject.markAsDeployingTo(manifest, versionedDockerArtifact, dockerVersion2, testEnvironment.name)
        clock.incrementBy(Duration.ofMinutes(5))
        subject.markAsSuccessfullyDeployedTo(manifest, versionedDockerArtifact, dockerVersion2, testEnvironment.name)
        clock.incrementBy(Duration.ofMinutes(9))
        // version 1 should be skipped, approve it again
        // FIXME: this will have a later timestamp, so the test below fails
        subject.approveVersionFor(manifest, versionedDockerArtifact, dockerVersion1, testEnvironment.name)
      }

      test("latest approved version is still the latest deployed") {
        val latestApproved = subject.latestVersionApprovedIn(manifest, versionedDockerArtifact, testEnvironment.name)
        expectThat(latestApproved).isNotNull().isEqualTo(dockerVersion2)
      }
    }

    context("a version has been pinned to an environment") {
      before {
        clock.incrementBy(Duration.ofHours(1))
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion1, stagingEnvironment.name)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, stagingEnvironment.name)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion2, stagingEnvironment.name)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion2, stagingEnvironment.name)
      }

      test("without a pin, latestVersionApprovedIn returns the latest approved version") {
        expectThat(subject.latestVersionApprovedIn(manifest, debianFromMainBranch, stagingEnvironment.name))
          .isEqualTo(debVersion2)
          .isNotEqualTo(pin1.version)
      }

      test("get env artifact version shows that artifact is not pinned") {
        val envArtifactSummary = subject.getArtifactSummaryInEnvironment(
          deliveryConfig = manifest,
          environmentName = pin1.targetEnvironment,
          artifactReference = debianFromMainBranch.reference,
          version = debVersion1
        )
        expectThat(envArtifactSummary)
          .isNotNull()
          .get { pinned }
          .isNull()
      }

      context("once pinned") {
        before {
          subject.pinEnvironment(manifest, pin1)
        }

        test("latestVersionApprovedIn prefers a pinned version over the latest approved version") {
          expectThat(subject.latestVersionApprovedIn(manifest, debianFromMainBranch, stagingEnvironment.name))
            .isEqualTo(debVersion1)
            .isEqualTo(pin1.version)
        }

        test("pinned version cannot be vetoed") {
          expectThat(
            subject.markAsVetoedIn(
              manifest,
              EnvironmentArtifactVeto(
                pin1.targetEnvironment,
                debianFromMainBranch.reference,
                pin1.version,
                "sheepy",
                "this pin is baaaaaad"
              )
            )
          )
            .isFalse()
        }

        test("getting pinned environments shows the pin") {
          val pins = subject.getPinnedEnvironments(manifest)
          expectThat(pins)
            .hasSize(1)
            .isEqualTo(
              listOf(
                PinnedEnvironment(
                  deliveryConfigName = manifest.name,
                  targetEnvironment = pin1.targetEnvironment,
                  artifact = debianFromMainBranch,
                  version = debVersion1,
                  pinnedBy = pin1.pinnedBy,
                  pinnedAt = clock.instant(),
                  comment = pin1.comment
                )
              )
            )
        }

        test("get env artifact version shows that artifact is pinned") {
          val envArtifactSummary = subject.getArtifactSummaryInEnvironment(
            deliveryConfig = manifest,
            environmentName = pin1.targetEnvironment,
            artifactReference = debianFromMainBranch.reference,
            version = debVersion1
          )
          expect {
            that(envArtifactSummary).isNotNull()
            that(envArtifactSummary?.pinned).isEqualTo(
              ActionMetadata(
                by = pin1.pinnedBy,
                at = clock.instant(),
                comment = pin1.comment
              )
            )
          }
        }
      }
    }
  }
}
