package com.netflix.spinnaker.keel.upsert

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.config.PersistenceRetryConfig
import com.netflix.spinnaker.keel.api.DeliveryConfig.Companion.MIGRATING_KEY
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.artifacts.branchStartsWith
import com.netflix.spinnaker.keel.api.artifacts.from
import com.netflix.spinnaker.keel.api.plugins.ResolvableResourceHandler
import com.netflix.spinnaker.keel.api.plugins.SupportedKind
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.diff.DefaultResourceDiffFactory
import com.netflix.spinnaker.keel.events.DeliveryConfigChangedNotification
import com.netflix.spinnaker.keel.exceptions.ValidationException
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.NoDeliveryConfigForApplication
import com.netflix.spinnaker.keel.persistence.OverwritingExistingResourcesDisallowed
import com.netflix.spinnaker.keel.persistence.PersistenceRetry
import com.netflix.spinnaker.keel.test.DummyResourceHandlerV1
import com.netflix.spinnaker.keel.test.DummyResourceSpec
import com.netflix.spinnaker.keel.test.TEST_API_V1
import com.netflix.spinnaker.keel.test.TEST_API_V2
import com.netflix.spinnaker.keel.test.deliveryArtifact
import com.netflix.spinnaker.keel.test.submittedDeliveryConfig
import com.netflix.spinnaker.keel.test.submittedResource
import com.netflix.spinnaker.keel.validators.DeliveryConfigValidator
import io.mockk.Runs
import io.mockk.called
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.context.ApplicationEventPublisher
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isFalse
import strikt.assertions.isTrue
import strikt.assertions.second
import org.springframework.core.env.Environment as SpringEnv

object DummyResourceHandlerV2NoCurrent :
  ResolvableResourceHandler<DummyResourceSpec, Map<String, DummyResourceSpec>>(emptyList()) {
  override val supportedKind =
    SupportedKind(TEST_API_V2.qualify("whatever"), DummyResourceSpec::class.java)

  override val eventPublisher: EventPublisher = mockk(relaxed = true)

  override suspend fun current(resource: Resource<DummyResourceSpec>): Map<String, DummyResourceSpec> {
    return emptyMap()
  }

  override suspend fun toResolvedType(resource: Resource<DummyResourceSpec>): Map<String, DummyResourceSpec> =
    emptyMap()
}

internal class DeliveryConfigUpserterTest {
  private val repository: KeelRepository = mockk()
  private val mapper: ObjectMapper = mockk()
  private val validator: DeliveryConfigValidator = mockk()
  private val publisher: ApplicationEventPublisher = mockk()
  private val springEnv: SpringEnv = mockk()
  private val persistenceRetry = PersistenceRetry(PersistenceRetryConfig())
  private val dummyResourceHandler = spyk(DummyResourceHandlerV1)
  private val dummyResourceHandlerV2 = spyk(DummyResourceHandlerV2NoCurrent)
  private val resourceHandlers = listOf(dummyResourceHandler, dummyResourceHandlerV2)

  private val subject = DeliveryConfigUpserter(
    repository = repository,
    mapper = mapper,
    validator = validator,
    publisher = publisher,
    springEnv = springEnv,
    persistenceRetry = persistenceRetry,
    diffFactory = DefaultResourceDiffFactory(),
    resourceHandlers = resourceHandlers,
  )

  private val submittedDeliveryConfig = submittedDeliveryConfig()
  private val deliveryConfig = submittedDeliveryConfig.toDeliveryConfig()

  @BeforeEach
  fun setupMocks() {
    every {
      repository.getDeliveryConfigForApplication(any())
    } returns deliveryConfig

    every {
      validator.validate(any())
    } just Runs

    every {
      repository.upsertDeliveryConfig(any<SubmittedDeliveryConfig>())
    } returns deliveryConfig

    every {
      springEnv.getProperty("keel.notifications.send-config-changed", Boolean::class.java, true)
    } returns true

    every {
      publisher.publishEvent(any<Object>())
    } just Runs
  }

  @Test
  fun `no upsert if validation fails`() {
    every {
      validator.validate(any())
    }.throws(ValidationException("bad config"))

    expectThrows<ValidationException> {
      subject.upsertConfig(submittedDeliveryConfig, allowResourceOverwriting = true)
    }
    verify(exactly = 0) { repository.upsertDeliveryConfig(any<SubmittedDeliveryConfig>()) }
  }

  @Test
  fun `can upsert a valid delivery config`() {
    expectThat(subject.upsertConfig(submittedDeliveryConfig, allowResourceOverwriting = true)).second.isFalse()
    verify { repository.upsertDeliveryConfig(submittedDeliveryConfig) }
    verify(exactly = 0) { publisher.publishEvent(any<Object>()) } // No diff
  }

  @Test
  fun `notify on config changes`() {
    every {
      repository.getDeliveryConfigForApplication(any())
    } returns deliveryConfig.copy(artifacts = setOf(deliveryArtifact(name = "differentArtifact")))

    subject.upsertConfig(submittedDeliveryConfig, allowResourceOverwriting = true)

    verify { repository.upsertDeliveryConfig(submittedDeliveryConfig) }
    verify { publisher.publishEvent(any<DeliveryConfigChangedNotification>()) }
  }

  @Test
  fun `ignores changes in object metadata when comparing current with new delivery config`() {
    every {
      repository.getDeliveryConfigForApplication(any())
    } returns deliveryConfig.run {
      copy(
        metadata = metadata + ("another" to "value"),
        environments = environments.map { it.copy().addMetadata("another" to "value") }.toSet()
      )
    }

    subject.upsertConfig(submittedDeliveryConfig, allowResourceOverwriting = true)

    verify { publisher wasNot called }
  }

  @Test
  fun `ignores preview objects when comparing current with new delivery config`() {
    every {
      repository.getDeliveryConfigForApplication(any())
    } returns deliveryConfig.run {
      copy(
        artifacts = artifacts + DockerArtifact("myimage", from = from(branchStartsWith("feature.")), isPreview = true),
        environments = environments + Environment("preview", isPreview = true)
      )
    }

    subject.upsertConfig(submittedDeliveryConfig, allowResourceOverwriting = true)

    verify { publisher wasNot called }
  }

  @Test
  fun `mark config as new if there is no existing config`() {
    every {
      repository.getDeliveryConfigForApplication(any())
    }.throws(NoDeliveryConfigForApplication(deliveryConfig.application))

    expectThat(subject.upsertConfig(submittedDeliveryConfig, allowResourceOverwriting = true)).second.isTrue()
  }

  @Test
  fun `allow upserting if resource current resolver is an empty map`() {
    every {
      repository.getDeliveryConfigForApplication(any())
    }.throws(NoDeliveryConfigForApplication(deliveryConfig.application))

    expectThat(
      subject.upsertConfig(
        submittedDeliveryConfig(resource = submittedResource(kind = TEST_API_V2.qualify("whatever"))),
        allowResourceOverwriting = false
      )
    ).second.isTrue()
  }


  @Test
  fun `do not allow upserting if overwriting existing resources for a new app`() {
    every {
      repository.getDeliveryConfigForApplication(any())
    }.throws(NoDeliveryConfigForApplication(deliveryConfig.application))

    expectThrows<OverwritingExistingResourcesDisallowed> {
      (subject.upsertConfig(
        submittedDeliveryConfig,
        allowResourceOverwriting = false
      ))
    }
  }

  @Test
  fun `mark config as new if the app is migrating`() {
    every {
      repository.getDeliveryConfigForApplication(any())
    } returns deliveryConfig.run {
      copy(
        metadata = metadata + (MIGRATING_KEY to true),
      )
    }

    expectThat(subject.upsertConfig(submittedDeliveryConfig, allowResourceOverwriting = true)).second.isTrue()
  }
}
