package com.netflix.spinnaker.keel.persistence

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.ArtifactStatus.RELEASE
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.artifacts.fromBranch
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.core.api.DependsOnConstraint
import com.netflix.spinnaker.keel.core.api.ManualJudgementConstraint
import com.netflix.spinnaker.keel.core.api.PromotionStatus.CURRENT
import com.netflix.spinnaker.keel.core.api.PromotionStatus.PENDING
import com.netflix.spinnaker.keel.core.api.PromotionStatus.SKIPPED
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import strikt.api.expect
import strikt.assertions.containsExactly
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isNull
import java.time.Clock

/**
 * In the artifact repository we have several methods that generate summary views of data
 * that are returned in the /application/{application} endpoint.
 * This class tests some of that data generation.
 */
abstract class ApplicationSummaryGenerationTests<T : ArtifactRepository> : JUnit5Minutests {

  abstract fun factory(clock: Clock): T

  val clock = MutableClock()

  open fun T.flush() {}

  data class Fixture<T : ArtifactRepository>(
    val subject: T
  ) {
    val debianArtifact = DebianArtifact(
      name = "keeldemo",
      deliveryConfigName = "my-manifest",
      reference = "my-artifact",
      vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2")),
      from = fromBranch("main")
    )
    val dockerArtifact = DockerArtifact(
      name = "myorg/myapp",
      deliveryConfigName = "my-manifest",
      reference = "my-docker-artifact",
      from = fromBranch("main")
    )
    val environmentA = Environment("aa")
    val environmentB = Environment(
      name = "bb",
      constraints = setOf(DependsOnConstraint("aa"), ManualJudgementConstraint())
    )
    val manifest = DeliveryConfig(
      name = "my-manifest",
      application = "fnord",
      serviceAccount = "keel@spinnaker",
      artifacts = setOf(debianArtifact, dockerArtifact),
      environments = setOf(environmentA, environmentB)
    )
    val debVersion1 = "keeldemo-1.0.1-h11.1a1a1a1"
    val debVersion2 = "keeldemo-1.0.2-h12.2b2b2b2"
    val dockerVersion = "doesn't-really-matter"
  }

  open fun Fixture<T>.persist() {
    with(subject) {
      register(debianArtifact)
      register(dockerArtifact)
      setOf(debVersion1, debVersion2).forEach {
        storeArtifactVersion(debianArtifact.toArtifactVersion(it, RELEASE))
      }
      storeArtifactVersion(dockerArtifact.toArtifactVersion(dockerVersion))
    }
    persist(manifest)
  }

  abstract fun persist(manifest: DeliveryConfig)

  fun tests() = rootContext<Fixture<T>> {
    fixture { Fixture(factory(clock)) }

    before {
      persist()
    }

    after {
      subject.flush()
    }

    context("artifact 1 skipped in envA, manual judgement before envB") {
      before {
        // version 1 and 2 are approved in env A
        subject.approveVersionFor(manifest, debianArtifact, debVersion1, environmentA.name)
        subject.approveVersionFor(manifest, debianArtifact, debVersion2, environmentA.name)
        // only version 2 is approved in env B
        subject.approveVersionFor(manifest, debianArtifact, debVersion2, environmentB.name)
        // version 1 has been skipped in env A by version 2
        subject.markAsSkipped(manifest, debianArtifact, debVersion1, environmentA.name, debVersion2)
        // version 2 was successfully deployed to both envs
        subject.markAsSuccessfullyDeployedTo(manifest, debianArtifact, debVersion2, environmentA.name)
        subject.markAsSuccessfullyDeployedTo(manifest, debianArtifact, debVersion2, environmentB.name)
      }

      test("skipped versions don't get a pending status in the next env") {
        val envAVersions = subject.getAllVersionsForEnvironment(debianArtifact, manifest, environmentA.name)
        val envBVersions = subject.getAllVersionsForEnvironment(debianArtifact, manifest, environmentB.name)
        expect {
          that(envAVersions).hasSize(2)
          that(envAVersions.find { it.status == CURRENT }!!.publishedArtifact.version).isEqualTo(debVersion2)
          that(envAVersions.find { it.status == SKIPPED }!!.publishedArtifact.version).isEqualTo(debVersion1)

          that(envBVersions).hasSize(1)
          that(envBVersions.find { it.status == CURRENT }!!.publishedArtifact.version).isEqualTo(debVersion2)
          that(envBVersions.find { it.status == PENDING }).isNull()
        }
      }
    }

    context("version of docker artifact filtered by source is deployed in envA") {
      before {
        // version approved in env A
        subject.approveVersionFor(manifest, dockerArtifact, dockerVersion, environmentA.name)
        subject.markAsSuccessfullyDeployedTo(manifest, dockerArtifact, dockerVersion, environmentA.name)
      }

      test("version is found to be CURRENT") {
        val envAVersions = subject.getAllVersionsForEnvironment(dockerArtifact, manifest, environmentA.name)
        expect {
          that(envAVersions.find { it.status == CURRENT }!!.publishedArtifact.version).isEqualTo(dockerVersion)
        }
      }
    }
  }
}
