package com.netflix.spinnaker.keel.titus

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.Highlander
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.SimpleLocations
import com.netflix.spinnaker.keel.api.SimpleRegionSpec
import com.netflix.spinnaker.keel.api.artifacts.DockerImage
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.api.titus.TitusServerGroupSpec
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.docker.DigestProvider
import com.netflix.spinnaker.keel.docker.ReferenceProvider
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.resolvers.DesiredVersionResolver
import com.netflix.spinnaker.keel.resolvers.NoDeployableVersionForEnvironment
import com.netflix.spinnaker.keel.test.TEST_API_V1
import com.netflix.spinnaker.keel.test.dockerArtifact
import com.netflix.spinnaker.keel.titus.TitusImageResolver.Companion.TITUS_REGISTRY_IMAGE_TTL
import com.netflix.spinnaker.keel.titus.exceptions.ImageTooOld
import com.netflix.spinnaker.keel.titus.exceptions.NoDigestFound
import com.netflix.spinnaker.keel.titus.registry.TitusRegistryService
import com.netflix.spinnaker.time.MutableClock
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThrows
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import java.time.Duration
import io.mockk.coEvery as every

class TitusImageResolverTests {
  private val repository: KeelRepository = mockk()
  private val cloudDriverService: CloudDriverService = mockk()
  private val cloudDriverCache: CloudDriverCache = mockk()
  private val titusRegistryService: TitusRegistryService = mockk()
  private val clock = MutableClock()
  private val desiredVersionResolver: DesiredVersionResolver = mockk()

  private val subject = TitusImageResolver(repository, clock, cloudDriverService, cloudDriverCache, titusRegistryService, desiredVersionResolver)
  private val dockerArtifact = dockerArtifact()
  private val digest = "sha256:2763a2b9d53e529c62b326b7331d1b44aae344be0b79ff64c74559c5c96b76b7"
  private val dockerImage = DockerImage(
    account = "account",
    repository = "any",
    tag = "1.0.0",
    digest = digest,
  )

  private val baseSpec = TitusClusterSpec(
    moniker = Moniker("waffles"),
    locations = SimpleLocations(
      account = "account",
      regions = setOf(SimpleRegionSpec("east"))
    ),
    container = ReferenceProvider(reference = dockerArtifact.reference), //DigestProvider(organization = "waffles", image = "butter", digest = "1234567890"),
    _defaults = TitusServerGroupSpec(
      capacity = ClusterSpec.CapacitySpec(1, 4, 2)
    ),
    rolloutWith = null,
    deployWith = Highlander()
  )

  val referenceResource = generateResource(baseSpec)
  val referenceDeliveryConfig = generateDeliveryConfig(referenceResource, setOf(dockerArtifact))

  private fun generateResource(spec: TitusClusterSpec) =
    Resource(
      kind = TEST_API_V1.qualify("whatever"),
      spec = spec,
      metadata = mapOf(
        "id" to "${TEST_API_V1}:sample:sample-resource",
        "application" to "myapp",
        "serviceAccount" to "keel@spinnaker"
      )
    )

  private fun generateDeliveryConfig(resource: Resource<TitusClusterSpec>, artifacts: Set<DockerArtifact>): DeliveryConfig {
    val env = Environment(
      name = "test",
      resources = setOf(resource)
    )
    return DeliveryConfig(
      name = "mydeliveryconfig",
      application = "keel",
      serviceAccount = "keel@spinnaker",
      artifacts = artifacts,
      environments = setOf(env)
    )
  }

  @BeforeEach
  fun setup() {

    every {
      cloudDriverCache.getRegistryForTitusAccount(any())
    } returns "testregistry"
  }

  @Test
  fun `reference provider - no deployable versions approved`() {
    every {
      titusRegistryService.findImages(dockerArtifact.name, any(), "1.0.0")
    } returns listOf(dockerImage)

    every { repository.deliveryConfigFor(referenceResource.id) } returns referenceDeliveryConfig
    every { repository.environmentFor(referenceResource.id) } returns referenceDeliveryConfig.environments.first()
    every { desiredVersionResolver.getDesiredVersion(
      deliveryConfig = referenceDeliveryConfig,
      environment = referenceDeliveryConfig.environments.first(),
      artifact = dockerArtifact
    ) } throws NoDeployableVersionForEnvironment(dockerArtifact, "test", emptyList())

    expectThrows<NoDeployableVersionForEnvironment> { subject.invoke(referenceResource) }
  }

  @Test
  fun `reference provider - version approved & deployable`() {
    every {
      titusRegistryService.findImages(dockerArtifact.name, any(), "1.0.0")
    } returns listOf(dockerImage)

    every { repository.deliveryConfigFor(referenceResource.id) } returns referenceDeliveryConfig
    every { repository.environmentFor(referenceResource.id) } returns referenceDeliveryConfig.environments.first()
    every { desiredVersionResolver.getDesiredVersion(
      deliveryConfig = referenceDeliveryConfig,
      environment = referenceDeliveryConfig.environments.first(),
      artifact = dockerArtifact
    ) } returns "1.0.0"

    val resolvedResource = subject.invoke(referenceResource)
    expect {
      that(resolvedResource.spec.container).isA<DigestProvider>()
      that(resolvedResource.spec.container as DigestProvider).get { digest }.isEqualTo(digest)
    }
  }

  @Test
  fun `throws NoDigestFound if digest not found and published version not known`() {
    every {
      titusRegistryService.findImages(dockerArtifact.name, "test", "1.0.0")
    } returns emptyList()
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
      titusRegistryService.findImages(dockerArtifact.name, "test", "1.0.0")
    } returns emptyList()
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
      titusRegistryService.findImages(dockerArtifact.name, "test", "1.0.0")
    } returns emptyList()
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
}
