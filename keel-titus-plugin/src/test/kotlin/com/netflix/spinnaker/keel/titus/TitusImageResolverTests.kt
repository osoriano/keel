package com.netflix.spinnaker.keel.titus

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.config.Features.OPTIMIZED_DOCKER_FLOW

import com.netflix.spinnaker.keel.api.artifacts.DockerImage
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.test.dockerArtifact
import com.netflix.spinnaker.keel.titus.TitusImageResolver.Companion.TITUS_REGISTRY_IMAGE_TTL
import com.netflix.spinnaker.keel.titus.exceptions.ImageTooOld
import com.netflix.spinnaker.keel.titus.exceptions.NoDigestFound
import com.netflix.spinnaker.keel.titus.registry.TitusRegistryService
import com.netflix.spinnaker.time.MutableClock
import io.mockk.coEvery as every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.core.env.ConfigurableEnvironment
import strikt.api.expectCatching
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isSuccess
import java.time.Duration

class TitusImageResolverTests {
  private val repository: KeelRepository = mockk()
  private val cloudDriverService: CloudDriverService = mockk()
  private val cloudDriverCache: CloudDriverCache = mockk()
  private val titusRegistryService: TitusRegistryService = mockk()
  private val clock = MutableClock()
  private val featureToggles: FeatureToggles = mockk()
  private val subject = TitusImageResolver(repository, clock, cloudDriverCache, cloudDriverService, titusRegistryService, featureToggles)
  private val dockerArtifact = dockerArtifact()

  @BeforeEach
  fun setup() {
    every {
      cloudDriverService.findDockerImages(any(), any(), any())
    } returns emptyList()

    every {
      cloudDriverCache.getRegistryForTitusAccount(any())
    } returns "testregistry"

    every {
      cloudDriverCache.getAwsAccountNameForTitusAccount(any())
    } returns "test"

    every {
      featureToggles.isEnabled(OPTIMIZED_DOCKER_FLOW)
    } returns false
  }

  @Test
  fun `throws NoDigestFound if digest not found and published version not known`() {
    every {
      repository.getArtifactVersion(dockerArtifact, any())
    } returns null

    expectCatching {
      subject.getDigest("test", dockerArtifact, "1.0.0")
    }.isFailure()
      .isA<NoDigestFound>()
  }

  @Test
  fun `throws NoDigestFound if digest not found and published version within TTL`() {
    every {
      repository.getArtifactVersion(dockerArtifact, any())
    } returns dockerArtifact.toArtifactVersion(
      version = "1.0.0",
      createdAt = clock.instant() - TITUS_REGISTRY_IMAGE_TTL + Duration.ofDays(5)
    )

    expectCatching {
      subject.getDigest("test", dockerArtifact, "1.0.0")
    }.isFailure()
      .isA<NoDigestFound>()
  }

  @Test
  fun `throws ImageTooOld if digest not found and published version is too old`() {
    every {
      repository.getArtifactVersion(dockerArtifact, any())
    } returns dockerArtifact.toArtifactVersion(
      version = "1.0.0",
      createdAt = clock.instant() - TITUS_REGISTRY_IMAGE_TTL - Duration.ofDays(5)
    )

    expectCatching {
      subject.getDigest("test", dockerArtifact, "1.0.0")
    }.isFailure()
      .isA<ImageTooOld>()
  }

  @Test
  fun `uses Titus registry cache when optimized Docker flow is enabled`() {
    every {
      featureToggles.isEnabled(OPTIMIZED_DOCKER_FLOW)
    } returns true

    every {
      titusRegistryService.findImages(dockerArtifact.name, "test", "1.0.0")
    } returns listOf(
      DockerImage("test", dockerArtifact.name, "1.0.0", digest = "asdf1234")
    )

    expectCatching {
      subject.getDigest("test", dockerArtifact, "1.0.0")
    }.isSuccess()
      .isEqualTo("asdf1234")

    verify {
      titusRegistryService.findImages(dockerArtifact.name, "test", "1.0.0")
    }
  }
}
