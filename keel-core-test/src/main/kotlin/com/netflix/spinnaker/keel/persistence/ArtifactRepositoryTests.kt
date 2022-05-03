package com.netflix.spinnaker.keel.persistence

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.ArtifactMetadata
import com.netflix.spinnaker.keel.api.artifacts.ArtifactOriginFilter
import com.netflix.spinnaker.keel.api.artifacts.BranchFilter
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.Job
import com.netflix.spinnaker.keel.api.artifacts.PullRequest
import com.netflix.spinnaker.keel.api.artifacts.Repo
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy.SEMVER_JOB_COMMIT_BY_JOB
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.core.api.ActionMetadata
import com.netflix.spinnaker.keel.core.api.ArtifactVersionVetoData
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactPin
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVeto
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVetoes
import com.netflix.spinnaker.keel.core.api.PromotionStatus.APPROVED
import com.netflix.spinnaker.keel.core.api.PromotionStatus.CURRENT
import com.netflix.spinnaker.keel.core.api.PromotionStatus.DEPLOYING
import com.netflix.spinnaker.keel.core.api.PromotionStatus.PREVIOUS
import com.netflix.spinnaker.keel.core.api.PromotionStatus.SKIPPED
import com.netflix.spinnaker.keel.core.api.PromotionStatus.VETOED
import com.netflix.spinnaker.keel.persistence.ArtifactRepositoryTests.ArtifactRepositoryFixture
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.mockk
import org.springframework.context.ApplicationEventPublisher
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.containsExactly
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.doesNotContainKey
import strikt.assertions.first
import strikt.assertions.getValue
import strikt.assertions.hasSize
import strikt.assertions.isA
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isFalse
import strikt.assertions.isNotEmpty
import strikt.assertions.isNotEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import strikt.assertions.isSuccess
import strikt.assertions.isTrue
import java.time.Instant

abstract class ArtifactRepositoryTests<T : ArtifactRepository> : JUnit5Minutests {
  val publisher: ApplicationEventPublisher = mockk(relaxed = true)

  val clock = MutableClock()

  abstract fun factory(): T

  abstract fun deliveryConfigRepository(): DeliveryConfigRepository

  open fun T.flush() {}

  open class ArtifactRepositoryFixture<T : ArtifactRepository>(
    open val subject: T,
    open val deliveryConfigRepository: DeliveryConfigRepository,
    open val clock: MutableClock,
  ) {
    val versionedDockerArtifact = DockerArtifact(
      name = "docker",
      deliveryConfigName = "my-manifest",
      reference = "docker-artifact",
      tagVersionStrategy = SEMVER_JOB_COMMIT_BY_JOB
    )

    val debianFromMainBranch = DebianArtifact(
      name = "keeldemo",
      deliveryConfigName = "my-manifest",
      reference = "main",
      vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2")),
      from = ArtifactOriginFilter(
        branch = BranchFilter(
          name = "main"
        )
      )
    )

    val debianFromBranchPrefix = DebianArtifact(
      name = "keeldemo",
      deliveryConfigName = "my-manifest",
      reference = "feature-branch",
      vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2")),
      from = ArtifactOriginFilter(
        branch = BranchFilter(
          startsWith = "feature/"
        )
      )
    )

    val debianFromBranchPattern = DebianArtifact(
      name = "keeldemo",
      deliveryConfigName = "my-manifest",
      reference = "feature-branch-pattern",
      vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2")),
      from = ArtifactOriginFilter(
        branch = BranchFilter(
          regex = ".*feature.*"
        )
      )
    )

    val debianFromPullRequest = DebianArtifact(
      name = "keeldemo",
      deliveryConfigName = "my-manifest",
      reference = "feature-pr",
      vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2")),
      from = ArtifactOriginFilter(
        pullRequestOnly = true
      )
    )

    val debianFromPullRequestAndBranch = DebianArtifact(
      name = "keeldemo",
      deliveryConfigName = "my-manifest",
      reference = "feature-pr-and-branch",
      vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2")),
      from = ArtifactOriginFilter(
        branch = BranchFilter(
          name = "feature"
        ),
        pullRequestOnly = true
      )
    )

    val testEnvironment = Environment("test")
    val stagingEnvironment = Environment("staging")
    open val manifest = DeliveryConfig(
      name = "my-manifest",
      application = "fnord",
      serviceAccount = "keel@spinnaker",
      artifacts = setOf(
        versionedDockerArtifact,
        debianFromMainBranch,
        debianFromBranchPattern,
        debianFromPullRequest,
        debianFromPullRequestAndBranch
      ),
      environments = setOf(testEnvironment, stagingEnvironment)
    )
    val debVersion1 = "keeldemo-1.0.0-h8.41595c4"
    val debVersion2 = "keeldemo-1.0.0-h9.3d2c8ff"
    val debVersion3 = "keeldemo-1.0.0-h10.1d2d542"
    val versionOnly = "0.0.1~dev.8-h8.41595c4"
    open val debVersions = listOf(debVersion1, debVersion2, debVersion3)

    val dockerVersion = "master-h12.4ea8a9d"
    open val dockerVersions = listOf(dockerVersion)

    val buildMetadata = BuildMetadata(
      id = 1,
      uid = "1234",
      startedAt = "2020-11-24T04:44:04.000Z",
      completedAt = "2020-11-25T03:04:02.259Z",
      job = Job(
        name = "job bla bla",
        link = "enkins.com"
      ),
      number = "1"
    )

    val gitMetadata = GitMetadata(
      commit = "a15p0",
      author = "keel-user",
      repo = Repo(
        name = "keel",
        link = ""
      ),
      commitInfo = Commit(
        sha = "a15p0",
        message = "this is a commit message",
        link = ""
      ),
      branch = "main",
      project = "spkr",
    )

    val artifactMetadata = ArtifactMetadata(
      buildMetadata,
      gitMetadata
    )

    val limit = 15

    fun persist(storeVersions: Boolean = false) {
      with(subject) {
        register(debianFromMainBranch)
        register(debianFromBranchPrefix)
        register(debianFromBranchPattern)
        register(debianFromPullRequest)
        register(debianFromPullRequestAndBranch)
        register(versionedDockerArtifact)

        if (storeVersions) {
          debVersions.forEach {
            storeArtifactVersionWithMetadata(debianFromMainBranch, it)
          }

          dockerVersions.forEach {
            storeArtifactVersion(versionedDockerArtifact.toArtifactVersion(it, null, clock.tickMinutes(1)))
          }
        }
      }
      persist(manifest)
    }

    fun persist(manifest: DeliveryConfig) {
      deliveryConfigRepository.store(manifest)
    }

    fun versionsIn(environment: Environment, artifact: DeliveryArtifact) =
      subject.getAllVersionsForEnvironment(artifact, manifest, environment.name)
        .groupBy {it.status }
        .mapValues { it.value.map { v -> v.publishedArtifact.version } }
  }

  fun tests() = rootContext<ArtifactRepositoryFixture<T>> {
    fixture { ArtifactRepositoryFixture(factory(), deliveryConfigRepository(), clock) }

    after {
      subject.flush()
    }

    context("the artifact is unknown") {
      test("the artifact is not registered") {
        expectThat(subject.isRegistered(debianFromMainBranch.name, debianFromMainBranch.type)).isFalse()
      }

      test("storing a new version throws an exception") {
        expectThrows<NoSuchArtifactException> {
          subject.storeArtifactVersion(debianFromMainBranch.toArtifactVersion(debVersion1))
        }
      }

      test("trying to get versions throws an exception") {
        expectThrows<NoSuchArtifactException> {
          subject.versions(debianFromMainBranch, limit)
        }
      }
    }

    context("the artifact is known") {
      before {
        subject.register(debianFromMainBranch)
      }

      test("VM options are persisted and read correctly") {
        expectThat(subject.get(debianFromMainBranch.name, debianFromMainBranch.type, debianFromMainBranch.deliveryConfigName!!))
          .hasSize(1)
          .first()
          .isA<DebianArtifact>()
          .get { vmOptions }
          .isEqualTo(debianFromMainBranch.vmOptions)
      }

      test("re-registering the same artifact does not raise an exception") {
        subject.register(debianFromMainBranch)

        expectThat(subject.isRegistered(debianFromMainBranch.name, debianFromMainBranch.type)).isTrue()
      }

      test("changing an artifact name works") {
        subject.register(debianFromMainBranch.copy(name = "keeldemo-but-a-different-name"))
        val artifact = subject.get(debianFromMainBranch.deliveryConfigName!!, debianFromMainBranch.reference)
        expectThat(artifact.name).isEqualTo("keeldemo-but-a-different-name")
      }

      context("no versions exist") {
        test("listing versions returns an empty list") {
          expectThat(subject.versions(debianFromMainBranch, limit)).isEmpty()
        }

        test("no version is deploying") {
          expectThat(subject.isDeployingTo(manifest, testEnvironment.name)).isFalse()
        }
      }

      context("an artifact version already exists") {
        before {
          storeArtifactVersionWithMetadata(debianFromMainBranch, debVersion1)
        }

        test("registering the same version will update the git/build metadata") {
          val result = storeArtifactVersionWithMetadata(debianFromMainBranch, debVersion1)
          expectThat(result).isTrue()
          expectThat(subject.versions(debianFromMainBranch, limit)).hasSize(1)
        }

        test("adding a new version adds it to the list") {
          val result = storeArtifactVersionWithMetadata(debianFromMainBranch, debVersion2)

          expectThat(result).isTrue()
          expectThat(subject.versions(debianFromMainBranch, limit).map { it.version })
            .containsExactly(debVersion2, debVersion1)
        }
      }

      context("sorting is consistent by artifact publication time") {
        before {
          debVersions
            .forEach {
              storeArtifactVersionWithMetadata(debianFromMainBranch, it)
            }
        }

        test("versions are returned newest first") {
          expect {
            that(subject.versions(debianFromMainBranch, limit).map { it.version })
              .isEqualTo(debVersions.reversed())
          }
        }
      }

      context("limiting versions works") {
        before {
          (1..100).map { "1.0.$it" }.forEach {
            storeArtifactVersionWithMetadata(debianFromMainBranch, it)
          }
        }

        test("limit parameter takes effect when specified") {
          expectThat(subject.versions(debianFromMainBranch, 20)).hasSize(20)
          expectThat(subject.versions(debianFromMainBranch, 100)).hasSize(100)
        }
      }
    }

    context("artifact approval querying") {
      before {
        persist(storeVersions = true)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion3, testEnvironment.name)
      }

      test("we can query for all the versions and know they're approved") {
        expect {
          that(subject.isApprovedFor(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)).isTrue()
          that(subject.isApprovedFor(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)).isTrue()
          that(subject.isApprovedFor(manifest, debianFromMainBranch, debVersion3, testEnvironment.name)).isTrue()
        }
      }
    }

    context("getting all filters by type") {
      before {
        persist(storeVersions = true)
      }

      test("querying works") {
        expect {
          that(subject.getAll().size).isEqualTo(6)
          that(subject.getAll(DOCKER).size).isEqualTo(1)
          that(subject.getAll(DEBIAN).size).isEqualTo(5)
        }
      }
    }

    context("the latest version is vetoed") {
      before {
        subject.flush()
        persist()
        storeArtifactVersionWithMetadata(debianFromMainBranch, debVersion1)
        storeArtifactVersionWithMetadata(debianFromMainBranch, debVersion2)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion1, stagingEnvironment.name)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion2, stagingEnvironment.name)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion2, stagingEnvironment.name)
        subject.markAsVetoedIn(manifest, EnvironmentArtifactVeto(stagingEnvironment.name, debianFromMainBranch.reference, debVersion2, "tester", "you bad"))
        clock.tickMinutes(1)
      }

      test("latestVersionApprovedIn reflects the veto") {
        expectThat(subject.latestVersionApprovedIn(manifest, debianFromMainBranch, stagingEnvironment.name))
          .isEqualTo(debVersion1)
      }

      test("vetoedEnvironmentVersions reflects the veto") {
        expectThat(subject.vetoedEnvironmentVersions(manifest).map {
          it.copy(versions = it.versions.map { v -> v.copy(vetoedAt = null) }.toMutableSet())
        })
          .isEqualTo(
            listOf(
              EnvironmentArtifactVetoes(
                deliveryConfigName = manifest.name,
                targetEnvironment = stagingEnvironment.name,
                artifact = debianFromMainBranch,
                versions = mutableSetOf(ArtifactVersionVetoData(version = debVersion2, vetoedBy = "tester", vetoedAt = null, comment = "you bad"))
              )
            )
          )
      }

      test("version status reflects the veto") {
        expectThat(versionsIn(stagingEnvironment, debianFromMainBranch)) {
          getValue(SKIPPED).containsExactly(debVersion1)
          getValue(VETOED).containsExactly(debVersion2)
        }
      }

      test("current version is still the vetoed version") {
        expectThat(subject.getCurrentlyDeployedArtifactVersion(manifest, debianFromMainBranch, stagingEnvironment.name)?.version).isEqualTo(debVersion2)
      }

      test("correctly returns count of approved versions in time window") {
        val numVersions = subject.versionsInStatusBetween(
          manifest,
          debianFromMainBranch,
          stagingEnvironment.name,
          APPROVED,
          clock.instant().minusSeconds(120),
          clock.instant()
        )
        expectThat(numVersions).isEqualTo(2)
      }

      test("correctly returns count of current versions in time window") {
        val numVersions = subject.versionsInStatusBetween(
          manifest,
          debianFromMainBranch,
          stagingEnvironment.name,
          CURRENT,
          clock.instant().minusSeconds(120),
          clock.instant()
        )
        expectThat(numVersions).isEqualTo(1)
      }

      test("correctly returns count of recorded vetoed versions in time window") {
        val numVersions = subject.versionsInStatusBetween(
          manifest,
          debianFromMainBranch,
          stagingEnvironment.name,
          VETOED,
          clock.instant().minusSeconds(120),
          clock.instant()
        )
        expectThat(numVersions).isEqualTo(1)
      }

      test("can get all information about the versions") {
        val versions = subject.getAllVersionsForEnvironment(debianFromMainBranch, manifest, stagingEnvironment.name)
        expectThat(versions.size).isEqualTo(2)
        expectThat(versions.map { it.status }).containsExactlyInAnyOrder(listOf(VETOED, SKIPPED))
        expectThat(versions.first { it.status == SKIPPED }.publishedArtifact.version).isEqualTo(debVersion1)
        expectThat(versions.first { it.status == VETOED }.publishedArtifact.version).isEqualTo(debVersion2)
        expectThat(versions.first { it.status == VETOED }.isCurrent).isEqualTo(true)
      }

      test("get env artifact version shows that artifact is vetoed") {
        val envArtifactSummaries = subject.getArtifactSummariesInEnvironment(
          deliveryConfig = manifest,
          environmentName = stagingEnvironment.name,
          artifactReference = debianFromMainBranch.reference,
          versions = listOf(debVersion2)
        )
        expect {
          that(envArtifactSummaries).isNotEmpty()
          that(envArtifactSummaries.firstOrNull()?.vetoed).isEqualTo(ActionMetadata(by = "tester", at = clock.instant().minusSeconds(60), comment = "you bad"))
        }
      }

      test("unveto the vetoed version") {
        subject.deleteVeto(manifest, debianFromMainBranch, debVersion2, stagingEnvironment.name)

        val envArtifactSummaries = subject.getArtifactSummariesInEnvironment(
          deliveryConfig = manifest,
          environmentName = stagingEnvironment.name,
          artifactReference = debianFromMainBranch.reference,
          versions = listOf(debVersion2)
        )

        expectThat(subject.latestVersionApprovedIn(manifest, debianFromMainBranch, stagingEnvironment.name))
          .isEqualTo(debVersion2)
        expectThat(versionsIn(stagingEnvironment, debianFromMainBranch)) {
          doesNotContainKey(VETOED)
          doesNotContainKey(APPROVED)
          getValue(SKIPPED).containsExactly(debVersion1)
          getValue(CURRENT).containsExactly(debVersion2)
        }
        expect {
          that(envArtifactSummaries).isNotEmpty()
          that(envArtifactSummaries.firstOrNull()?.vetoed).isNull()
        }
      }
    }

    context("artifact metadata exists") {
      before {
        subject.register(debianFromMainBranch)
        subject.storeArtifactVersion(debianFromMainBranch.toArtifactVersion(debVersion1).copy(
          gitMetadata = artifactMetadata.gitMetadata,
          buildMetadata = artifactMetadata.buildMetadata
        ))
      }

      test("retrieves successfully") {
        val artifactVersion = subject.getArtifactVersion(debianFromMainBranch, debVersion1)!!

        expectThat(artifactVersion.buildMetadata)
          .isEqualTo(artifactMetadata.buildMetadata)

        expectThat(artifactVersion.gitMetadata)
          .isEqualTo(artifactMetadata.gitMetadata)
      }

      test("update with non-prefixed version works") {
        subject.storeArtifactVersion(debianFromMainBranch.toArtifactVersion(versionOnly).copy(
          gitMetadata = artifactMetadata.gitMetadata,
          buildMetadata = artifactMetadata.buildMetadata
        ))

        val artifactVersion = subject.getArtifactVersion(debianFromMainBranch, debVersion1)!!

        expectThat(artifactVersion.buildMetadata)
          .isEqualTo(artifactMetadata.buildMetadata)

        expectThat(artifactVersion.gitMetadata)
          .isEqualTo(artifactMetadata.gitMetadata)
      }
    }

    context("artifact creation timestamp exists") {
      val createdAt = Instant.now()

      before {
        subject.register(debianFromMainBranch)
        subject.storeArtifactVersion(debianFromMainBranch.toArtifactVersion(debVersion1, createdAt = createdAt))
      }

      test("retrieves timestamp successfully") {
        val artifactVersion = subject.getArtifactVersion(debianFromMainBranch, debVersion1)!!
        expectThat(artifactVersion.createdAt).isEqualTo(createdAt)
      }
    }

    context("artifact filtered by branch") {
      context("with branch name specified in the artifact spec") {
        // registers versions backwards to check that sorting by timestamp takes precedence
        val allVersions = (20 downTo 1).map { "keeldemo-any-string-$it" }

        before {
          subject.register(debianFromMainBranch)
          allVersions.forEachIndexed { index, version ->
            subject.storeArtifactVersion(
              debianFromMainBranch.toArtifactVersion(
                version = version,
                // half of the versions doesn't have a timestamp
                createdAt = if (index < 10) null else clock.tickMinutes(10)
              ).copy(
                gitMetadata = artifactMetadata.gitMetadata?.copy(
                  branch = debianFromMainBranch.from.branch!!.name
                ),
                buildMetadata = artifactMetadata.buildMetadata
              )
            )
          }
        }

        test("returns versions with matching branch sorted by timestamp") {
          val versions = subject.versions(debianFromMainBranch, 5)
          expectThat(versions.map { it.version })
            .containsExactly(allVersions.reversed().subList(0, 5))
        }

        test("skips artifacts without a timestamp") {
          val versions = subject.versions(debianFromMainBranch, 20)
          expectThat(versions.map { it.version })
            .containsExactly(allVersions.reversed().subList(0, 10))
        }
      }

      context("with branch prefix specified in the artifact spec") {
        // register versions backwards to check that sorting by timestamp takes precedence
        val allVersions = (20 downTo 1).map { "keeldemo-any-string-$it" }

        before {
          val prefix = debianFromBranchPrefix.from.branch!!.startsWith!!
          subject.register(debianFromBranchPrefix)
          allVersions.forEachIndexed { index, version ->
            storeArtifactVersionWithMetadata(
              artifact = debianFromBranchPrefix,
              version = version,
              branch = if (index < 10) "not-a-matching-branch" else "${prefix}my-feature-$index"
            )
          }
        }

        test("returns versions with matching branches sorted by timestamp") {
          val versions = subject.versions(debianFromBranchPrefix, 20)
          // only the first 10 versions have matching branches
          expectThat(versions.map { it.version })
            .containsExactly(allVersions.reversed().subList(0, 10))
        }
      }

      context("with branch pattern specified in the artifact spec") {
        // register versions backwards to check that sorting by timestamp takes precedence
        val allVersions = (20 downTo 1).map { "keeldemo-any-string-$it" }

        before {
          storeVersionsForDebianFromBranchPattern(allVersions)
        }

        test("returns versions with matching branches sorted by timestamp") {
          val versions = subject.versions(debianFromBranchPattern, 20)
          // first 5 have "a-non-matching-branch"
          expectThat(versions.map { it.version })
            .containsExactly(allVersions.reversed().subList(5, 20))
        }
      }
    }

    context("artifact filtered by pull request") {
      // register versions backwards to check that sorting by timestamp takes precedence
      val allVersions = (20 downTo 1).map { "keeldemo-any-string-$it" }

      before {
        storeVersionsForDebianFromPullRequest(allVersions)
      }

      test("returns versions with pull request sorted by timestamp") {
        val versions = subject.versions(debianFromPullRequest, 20)
        // half the "versions" don't have pull request info
        expectThat(versions.map { it.version })
          .containsExactly(allVersions.reversed().subList(0, 10))
      }
    }

    context("artifact filtered by pull request and branch") {
      // registers versions backwards to check that sorting by timestamp takes precedence
      val allVersions = (20 downTo 1).map { "keeldemo-any-string-$it" }

      before {
        subject.register(debianFromPullRequestAndBranch)
        allVersions.forEachIndexed { index, version ->
          subject.storeArtifactVersion(
            debianFromPullRequestAndBranch.toArtifactVersion(
              version = version,
              createdAt = clock.tickMinutes(10)
            ).copy(
              gitMetadata = artifactMetadata.gitMetadata!!.copy(
                // the last 5 versions don't have a matching branch
                branch = if (index in 0..4) null else debianFromPullRequestAndBranch.from.branch!!.name,
                // the 5 versions before that don't have pull request info
                pullRequest = if (index in 5..9) null else PullRequest(number = "111", url = "www.github.com/pr/111")
              ),
              buildMetadata = artifactMetadata.buildMetadata
            )
          )
        }
      }

      test("returns versions with pull request and matching branch sorted by timestamp") {
        val versions = subject.versions(debianFromPullRequestAndBranch, 20)
        // all versions should match
        expectThat(versions.map { it.version })
          .containsExactly(allVersions.reversed().subList(0, 10))
      }
    }

    context("artifact versions by promotion status") {
      before {
        persist(manifest)
        subject.register(debianFromMainBranch)
        storeArtifactVersionWithMetadata(debianFromMainBranch, debVersion1)
        storeArtifactVersionWithMetadata(debianFromMainBranch, debVersion2)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
      }

      test("no versions exists if not persisted") {
        expectThat(subject.getArtifactVersionByPromotionStatus(manifest, testEnvironment.name, debianFromMainBranch, PREVIOUS))
          .isNull()
      }

      test("get artifact versions for deploying status") {
        expectThat(subject.getArtifactVersionByPromotionStatus(manifest, testEnvironment.name, debianFromMainBranch, CURRENT)?.gitMetadata)
          .isEqualTo(artifactMetadata.gitMetadata)
      }

      test("get a single results (and newest) data per status") {
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        expectThat(subject.getArtifactVersionByPromotionStatus(manifest, testEnvironment.name, debianFromMainBranch, CURRENT)?.gitMetadata)
          .get { this?.commit }.isEqualTo(gitMetadata.commit)
      }

      test("get artifact version by promotion status and the version it replaced") {
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        expectThat(subject.getArtifactVersionByPromotionStatus(manifest, testEnvironment.name, debianFromMainBranch, PREVIOUS, debVersion2))
          .get { this?.version }.isEqualTo(debVersion1)
      }

      test("unsupported promotion status throws exception") {
        expectThrows<IllegalArgumentException> {
          subject.getArtifactVersionByPromotionStatus(manifest, testEnvironment.name, debianFromMainBranch, DEPLOYING)
        }
      }
    }


    context("can only mark a version as deployed once") {
      before {
        persist()
        storeArtifactVersionWithMetadata(debianFromMainBranch, debVersion1)
      }

      test("only one is true") {
        val results = mutableListOf<Boolean>()
        repeat(3) {
          subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
            .let(results::add)
        }
        expectThat(results.size).isEqualTo(3)
        expectThat(results.count { it }).isEqualTo(1)
      }
    }

    context("versions that weren't deployed") {
      before {
        persist(storeVersions = true)
        // approving version 1 then deploying version 2 makes version 1 skipped in testEnvironment
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        clock.tickMinutes(1)
      }

      test("correctly returns count of recorded skipped versions in time window") {
        val numVersions = subject.versionsInStatusBetween(
          manifest,
          debianFromMainBranch,
          testEnvironment.name,
          SKIPPED,
          clock.instant().minusSeconds(120),
          clock.instant()
        )
        expectThat(numVersions).isEqualTo(1)
      }

      test("can fetch skipped and pending versions") {
        val notDeployedVersions = subject.getNotYetDeployedVersionsInEnvironment(manifest, debianFromMainBranch.reference, testEnvironment.name)
        expect {
          that(notDeployedVersions).hasSize(2)
          that(notDeployedVersions.map { it.version }).containsExactlyInAnyOrder(listOf(debVersion1, debVersion3))
        }
      }

      test("can approve a skipped") {
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        expectThat(subject.isApprovedFor(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)).isTrue()
      }
    }

    context("pinned version") {
      before {
        persist(manifest)
        subject.register(debianFromMainBranch)
      }
      test("there isn't any pinned version in any environment") {
        expectThat(subject.getPinnedVersion(manifest, testEnvironment.name, debianFromMainBranch.reference))
          .isNull()
        expectThat(subject.getPinnedVersion(manifest, stagingEnvironment.name, debianFromMainBranch.reference))
          .isNull()
      }

      test("there is one pinned version in test, non in staging") {
        subject.pinEnvironment(manifest, EnvironmentArtifactPin(testEnvironment.name, debianFromMainBranch.reference, debVersion1, null, null))
        expectThat(subject.getPinnedVersion(manifest, testEnvironment.name, debianFromMainBranch.reference))
          .isEqualTo(debVersion1)
        expectThat(subject.getPinnedVersion(manifest, stagingEnvironment.name, debianFromMainBranch.reference))
          .isNull()
      }

      test("pinned two versions, get only the latest pinned version") {
        subject.pinEnvironment(manifest, EnvironmentArtifactPin(testEnvironment.name, debianFromMainBranch.reference, debVersion1, null, null))
        subject.pinEnvironment(manifest, EnvironmentArtifactPin(testEnvironment.name, debianFromMainBranch.reference, debVersion2, null, null))
        expectThat(subject.getPinnedVersion(manifest, testEnvironment.name, debianFromMainBranch.reference))
          .isEqualTo(debVersion2)
      }
    }

    context("latest artifact versions") {
      before {
        persist()
        storeArtifactVersionWithMetadata(debianFromMainBranch, debVersion1)
        storeArtifactVersionWithMetadata(debianFromMainBranch, debVersion2)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        clock.tickMinutes(1)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
      }
      test("get the current version by default") {
        expectThat(subject.getApprovedInEnvArtifactVersion(manifest, debianFromMainBranch, testEnvironment.name)?.version).isEqualTo(debVersion1)
      }

      test("we are not updating the approval time") {
        val deployedAt = subject.getArtifactSummariesInEnvironment(manifest, testEnvironment.name, debianFromMainBranch.reference, listOf(debVersion1)).firstOrNull()?.deployedAt
        expectThat(subject.getApprovedAt(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)).isNotNull().isNotEqualTo(deployedAt)
      }

      test("when exclude current is true, get the latest approved version which is not current") {
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        expectThat(subject.getApprovedInEnvArtifactVersion(manifest, debianFromMainBranch, testEnvironment.name, true)?.version).isEqualTo(debVersion1)
      }
    }

    context("last previously deployed version (replaced by the current version)") {
      before {
        persist()
        listOf(debVersion1, debVersion2, debVersion3).forEach {
          storeArtifactVersionWithMetadata(debianFromMainBranch, it)
          subject.approveVersionFor(manifest, debianFromMainBranch, it, testEnvironment.name)
          clock.tickMinutes(1)
        }
      }
      test("no current version") {
        expectThat(
          subject.getPreviouslyDeployedArtifactVersion(
            manifest,
            debianFromMainBranch,
            testEnvironment.name
          )
        ).isNull()
      }

      test("only one version was deployed") {
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        expectThat(
          subject.getPreviouslyDeployedArtifactVersion(
            manifest,
            debianFromMainBranch,
            testEnvironment.name
          )
        ).isNull()
      }

      test("there is one previous version. We don't roll back to a skipped version") {
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion3, testEnvironment.name)

        expectThat(
          subject.getArtifactPromotionStatus(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        ).isNotNull().isEqualTo(SKIPPED)

        expectThat(
          subject.getPreviouslyDeployedArtifactVersion(
            manifest,
            debianFromMainBranch,
            testEnvironment.name
          )
        ).isNotNull().get { version }.isEqualTo(debVersion1)
      }

      context("picking the right previous version") {
        before {
          listOf(debVersion1, debVersion2, debVersion3).forEach {
            subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, it, testEnvironment.name)
            clock.tickMinutes(1)
          }
        }

        test("picking the last version") {
          expectThat(
            subject.getPreviouslyDeployedArtifactVersion(
              manifest,
              debianFromMainBranch,
              testEnvironment.name
            )
          ).isNotNull().get { version }.isEqualTo(debVersion2) // We go to the version that replaced current
        }

        test("Still picking the right version after rolling back") {
          subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
          expectThat(
            subject.getPreviouslyDeployedArtifactVersion(
              manifest,
              debianFromMainBranch,
              testEnvironment.name
            )
          ).isNotNull().get { version }.isEqualTo(debVersion1) // We should go to the version that replace debVersion2 (and not to the newer version)
        }
      }
    }

    context("set approvedAt") {
      before {
        persist()
        storeArtifactVersionWithMetadata(debianFromMainBranch, debVersion1)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
      }
      test("we are setting the approvedAt") {
        val deployedAt = subject.getArtifactSummariesInEnvironment(manifest, testEnvironment.name, debianFromMainBranch.reference, listOf(debVersion1)).firstOrNull()?.deployedAt
        expectThat(subject.getApprovedAt(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)).isNotNull().isEqualTo(deployedAt)
      }
    }

    context("latest deployable version") {
      before {
        persist(storeVersions = true)
        clock.tickMinutes(1)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        clock.tickMinutes(1)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        clock.tickMinutes(1)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion3, testEnvironment.name)
        clock.tickMinutes(1)
      }

      test("no versions have started deploying, so version is null"){
        val version = subject.latestDeployableVersionIn(manifest, debianFromMainBranch, testEnvironment.name)
        expectThat(version).isNull()
      }

      test("there is one version deploying, so that version in returned") {
        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        val version = subject.latestDeployableVersionIn(manifest, debianFromMainBranch, testEnvironment.name)
        expectThat(version).isEqualTo(debVersion1)
      }

      test("deploying version is counted as a deployed version in the window") {
        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        clock.tickMinutes(1)
        val numDeployed = subject.versionsDeployedBetween(
          manifest,
          debianFromMainBranch,
          testEnvironment.name,
          clock.instant().minusSeconds(120),
          clock.instant()
        )
        expectThat(numDeployed).isEqualTo(1)
      }

      test("there is one version that is current, so that version is returned") {
        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        clock.tickMinutes(1)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)

        val version = subject.latestDeployableVersionIn(manifest, debianFromMainBranch, testEnvironment.name)
        expectThat(version).isEqualTo(debVersion1)
      }

      test("two versions have been deployed, we pick the correct one before and after a veto") {
        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        clock.tickMinutes(1)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        clock.tickMinutes(1)

        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        clock.tickMinutes(1)
        val version = subject.latestDeployableVersionIn(manifest, debianFromMainBranch, testEnvironment.name)
        expectThat(version).isEqualTo(debVersion2)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)

        val versionAfterSecondDeploy = subject.latestDeployableVersionIn(manifest, debianFromMainBranch, testEnvironment.name)
        expectThat(versionAfterSecondDeploy).isEqualTo(debVersion2)

        // veto version two
        subject.markAsVetoedIn(manifest, EnvironmentArtifactVeto(testEnvironment.name, debianFromMainBranch.reference, debVersion2, "me", "it bad"), true)
        val versionAfterVeto = subject.latestDeployableVersionIn(manifest, debianFromMainBranch, testEnvironment.name)
        expectThat(versionAfterVeto).isEqualTo(debVersion1)
      }

      test("there is a deploying, current, and previous version, we pick the deploying one because it is newest") {
        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        clock.tickMinutes(1)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        clock.tickMinutes(1)

        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        clock.tickMinutes(1)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        clock.tickMinutes(1)

        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion3, testEnvironment.name)

        val version = subject.latestDeployableVersionIn(manifest, debianFromMainBranch, testEnvironment.name)
        expectThat(version).isEqualTo(debVersion3)
      }
    }

    context("get deployment candidates") {
      before {
        persist(storeVersions = true)
        subject.register(debianFromMainBranch)

        // approving 3 first, then 2, then 1, to make sure that we order candidates not by approval time,
        //   but instead with the correct version sorting logic
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion3, testEnvironment.name)
        clock.tickMinutes(1)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        clock.tickMinutes(1)
        subject.approveVersionFor(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        clock.tickMinutes(1)
      }

      test("all approved versions are candidates") {
        val candidates = subject.deploymentCandidateVersions(manifest, debianFromMainBranch, testEnvironment.name)
        // order is important here, 3 is the newest.
        expectThat(candidates).containsExactly(debVersion3, debVersion2, debVersion1)
      }

      test("versions that have already started deploying or have been deployed are not candidates") {
        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        clock.tickMinutes(1)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        clock.tickMinutes(1)

        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        clock.tickMinutes(1)
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion2, testEnvironment.name)
        clock.tickMinutes(1)

        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion3, testEnvironment.name)
        clock.tickMinutes(1)

        val candidates = subject.deploymentCandidateVersions(manifest, debianFromMainBranch, testEnvironment.name)

        expectThat(candidates).isEmpty()
      }

      test("only approved versions newer than the latest deploying version are candidates") {
        subject.markAsDeployingTo(manifest, debianFromMainBranch, debVersion3, testEnvironment.name)
        clock.tickMinutes(1)

        val candidates = subject.deploymentCandidateVersions(manifest, debianFromMainBranch, testEnvironment.name)

        expectThat(candidates).isEmpty()
      }
    }

    context("recording and retrieving deployment timestamp") {
      before {
        persist(manifest)
        subject.register(debianFromMainBranch)
        listOf(debVersion1, debVersion2).forEach {
          storeArtifactVersionWithMetadata(debianFromMainBranch, it)
          subject.approveVersionFor(manifest, debianFromMainBranch, it, testEnvironment.name)
          clock.tickMinutes(1)
        }
      }

      test("can retrieve deployment timestamp for versions marked as deployed") {
        subject.markAsSuccessfullyDeployedTo(manifest, debianFromMainBranch, debVersion1, testEnvironment.name)
        expectCatching {
          subject.getDeployedAt(manifest, testEnvironment, debianFromMainBranch, debVersion1)
        }.isSuccess()
      }

      test("cannot retrieve deployment timestamp for versions not marked as deployed") {
        expectCatching {
          subject.getDeployedAt(manifest, testEnvironment, debianFromMainBranch, debVersion2)
        }.isFailure()
          .isA<NoSuchDeploymentException>()
      }
    }
  }
}

internal fun <T : ArtifactRepository> ArtifactRepositoryFixture<T>.storeArtifactVersionWithMetadata(
  artifact: DeliveryArtifact,
  version: String,
  branch: String = "main"
) =
  subject.storeArtifactVersion(
    artifact.toArtifactVersion(
      version = version,
      createdAt = clock.tickMinutes(10)
    ).copy(
      gitMetadata = artifactMetadata.gitMetadata?.copy(branch = branch),
      buildMetadata = artifactMetadata.buildMetadata
    )
  )

/**
 * This function creates `versions.size` versions for artifact `debianFilteredByBranchPattern`,
 * where only the first 15 of them have branch names that match the requested pattern
 */
internal fun <T : ArtifactRepository> ArtifactRepositoryFixture<T>.storeVersionsForDebianFromBranchPattern(versions: List<String>) {
  assert(versions.size == 20)
  subject.register(debianFromBranchPattern)
  versions.forEachIndexed { index, version ->
    storeArtifactVersionWithMetadata(
      artifact = debianFromBranchPattern,
      version = version,
      branch = when {
        index < 5 -> "my-feature-x"
        index < 10 -> "feature-branch-x"
        index < 15 -> "myfeature"
        else -> "a-non-matching-branch"
      }
    )
  }
}

/**
 * This function creates `versions.size` versions for artifact `debianFilteredByPullRequest`,
 * where only the first half have the PR info.
 */
internal fun <T : ArtifactRepository>  ArtifactRepositoryFixture<T>.storeVersionsForDebianFromPullRequest(versions: List<String>) {
  assert(versions.size == 20)
  subject.register(debianFromPullRequest)
  versions.forEachIndexed { index, version ->
    subject.storeArtifactVersion(
      debianFromPullRequest.toArtifactVersion(
        version = version,
        createdAt = clock.tickMinutes(10)
      ).copy(
        gitMetadata = artifactMetadata.gitMetadata!!.copy(
          // first half of the versions don't have pull request info
          pullRequest = if (index < 10) null else PullRequest(number = "111", url = "www.github.com/pr/111")
        ),
        buildMetadata = artifactMetadata.buildMetadata
      )
    )
  }
}
