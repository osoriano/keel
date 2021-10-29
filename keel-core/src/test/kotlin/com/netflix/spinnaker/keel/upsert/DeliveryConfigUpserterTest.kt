package com.netflix.spinnaker.keel.upsert

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.config.PersistenceRetryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.ArtifactOriginFilter
import com.netflix.spinnaker.keel.api.artifacts.branchStartsWith
import com.netflix.spinnaker.keel.api.artifacts.from
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.diff.DefaultResourceDiffFactory
import com.netflix.spinnaker.keel.events.DeliveryConfigChangedNotification
import com.netflix.spinnaker.keel.exceptions.ValidationException
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.PersistenceRetry
import com.netflix.spinnaker.keel.test.deliveryArtifact
import com.netflix.spinnaker.keel.test.submittedDeliveryConfig
import com.netflix.spinnaker.keel.validators.DeliveryConfigValidator
import io.mockk.Runs
import io.mockk.called
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.context.ApplicationEventPublisher
import org.springframework.core.env.Environment as SpringEnv
import strikt.api.expectThrows

internal class DeliveryConfigUpserterTest {
  private val repository: KeelRepository = mockk()
  private val mapper: ObjectMapper = mockk()
  private val validator: DeliveryConfigValidator = mockk()
  private val publisher: ApplicationEventPublisher = mockk()
  private val springEnv: SpringEnv = mockk()
  private val persistenceRetry = PersistenceRetry(PersistenceRetryConfig())

  private val subject = DeliveryConfigUpserter(
    repository = repository,
    mapper = mapper,
    validator = validator,
    publisher = publisher,
    springEnv = springEnv,
    persistenceRetry = persistenceRetry,
    DefaultResourceDiffFactory()
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
      repository.upsertDeliveryConfig(submittedDeliveryConfig)
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
      subject.upsertConfig(submittedDeliveryConfig)
    }
    verify(exactly = 0) { repository.upsertDeliveryConfig(any<SubmittedDeliveryConfig>()) }
  }

  @Test
  fun `can upsert a valid delivery config`() {
    subject.upsertConfig(submittedDeliveryConfig)
    verify { repository.upsertDeliveryConfig(submittedDeliveryConfig) }
    verify(exactly = 0) { publisher.publishEvent(any<Object>()) } // No diff
  }

  @Test
  fun `notify on config changes`() {
    every {
      repository.getDeliveryConfigForApplication(any())
    } returns deliveryConfig.copy(artifacts = setOf(deliveryArtifact(name = "differentArtifact")))

    subject.upsertConfig(submittedDeliveryConfig)

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

    subject.upsertConfig(submittedDeliveryConfig)

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

    subject.upsertConfig(submittedDeliveryConfig)

    verify { publisher wasNot called }
  }
}
