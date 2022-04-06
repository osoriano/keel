package com.netflix.spinnaker.keel.resolvers

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.constraints.DeploymentConstraintEvaluator
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.test.deliveryConfig
import com.netflix.spinnaker.keel.test.dockerArtifact
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo

class DesiredVersionResolverTests {
  val latestApprovedVersion = "10"
  val secondToLatestApprovedVersion = "9"
  val currentVersion = "5"

  val artifact = dockerArtifact()
  val env = Environment("test")
  val deliveryConfig = deliveryConfig(artifact = artifact, env = env)

  val repository = mockk<KeelRepository>() {
    every { latestVersionApprovedIn(any(), any(), any()) } returns latestApprovedVersion

    // candidate versions will be returned in descending order, newest first
    every { deploymentCandidateVersions(any(), any(), any()) } returns listOf(latestApprovedVersion, secondToLatestApprovedVersion, "8", "7", "6")
    every { getPinnedVersion(deliveryConfig, env.name, artifact.reference) } returns null
  }
  val featureToggles = mockk<FeatureToggles>()

  // checking of the deployment constraints is mocked so that we can test the desired version logic without
  //   wiring in the actual evaluators and testing that logic.
  val deploymentConstraintEvaluators: List<DeploymentConstraintEvaluator<*, *>> = emptyList()

  val subject = spyk(DesiredVersionResolver(repository, featureToggles, deploymentConstraintEvaluators))
  
  @Test
  fun `selects pinned version if there is one`(){
    every { repository.getPinnedVersion(deliveryConfig, env.name, artifact.reference) } returns "6"

    val desiredVersion = subject.getDesiredVersion(deliveryConfig, env, artifact)
    verify(exactly = 0) { repository.latestVersionApprovedIn(deliveryConfig, artifact, env.name) }
    verify(exactly = 0) { repository.deploymentCandidateVersions(deliveryConfig, artifact, env.name) }
    expectThat(desiredVersion).isEqualTo("6")
  }

  @Test
  fun `selects newest if it passes deployment constraints`() {
    coEvery { subject.checkConstraintWhenSpecified(artifact, deliveryConfig, latestApprovedVersion, env) } returns true

    val desiredVersion = subject.getDesiredVersion(deliveryConfig, env, artifact)
    expectThat(desiredVersion).isEqualTo(latestApprovedVersion)
  }

  @Test
  fun `marks older versions as skipped if a newer one is chosen`() {
    coEvery { subject.checkConstraintWhenSpecified(artifact, deliveryConfig, latestApprovedVersion, env) } returns true

    val desiredVersion = subject.getDesiredVersion(deliveryConfig, env, artifact)
    expectThat(desiredVersion).isEqualTo(latestApprovedVersion)
    verify(exactly = 1) { subject.markOlderVersionsAsSkipped(listOf(secondToLatestApprovedVersion, "8", "7", "6"), any(), any(), any()) }
  }

  @Test
  fun `checks and selects older versions if newest fails deployment constraints`() {
    coEvery { subject.checkConstraintWhenSpecified(artifact, deliveryConfig, latestApprovedVersion, env) } returns false
    coEvery { subject.checkConstraintWhenSpecified(artifact, deliveryConfig, secondToLatestApprovedVersion, env) } returns true

    val desiredVersion = subject.getDesiredVersion(deliveryConfig, env, artifact)
    expectThat(desiredVersion).isEqualTo(secondToLatestApprovedVersion)
  }

  @Test
  fun `selects the latest version that was ok to deploy if no approved versions pass deployment constraints`(){
    coEvery { subject.checkConstraintWhenSpecified(artifact, deliveryConfig, any(), env) } returns false
    coEvery { repository.latestDeployableVersionIn(deliveryConfig, artifact, env.name) } returns currentVersion

    val desiredVersion = subject.getDesiredVersion(deliveryConfig, env, artifact)
    expectThat(desiredVersion).isEqualTo(currentVersion)
  }

  @Test
  fun `throws exception if there are no deployable versions`() {
    coEvery { subject.checkConstraintWhenSpecified(artifact, deliveryConfig, any(), env) } returns false
    coEvery { repository.latestDeployableVersionIn(deliveryConfig, artifact, env.name) } returns null
    expectThrows<NoDeployableVersionForEnvironment> {
      subject.getDesiredVersion(deliveryConfig, env, artifact)
    }
  }
}
