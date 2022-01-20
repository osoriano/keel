package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.keel.KeelApplication
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.events.ArtifactPublishedEvent
import com.netflix.spinnaker.keel.scm.PrOpenedEvent
import com.netflix.spinnaker.keel.artifacts.WorkQueueProcessor
import com.netflix.spinnaker.keel.rest.ArtifactControllerTests.TestConfig
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.Runs
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.MOCK
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.context.support.GenericApplicationContext
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.ResultActions
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import strikt.api.expectThat
import strikt.assertions.isEqualTo

@SpringBootTest(webEnvironment = MOCK, classes = [TestConfig::class, KeelApplication::class])
@AutoConfigureMockMvc
internal class ArtifactControllerTests
@Autowired constructor(
  val mvc: MockMvc,
) : JUnit5Minutests {

  // Hack to mock ApplicationEventPublisher, which is handled as a special case in Spring (the application
  // context implements the interface). See https://github.com/spring-projects/spring-boot/issues/6060
  class TestConfig {
    @Bean
    @Primary
    fun genericApplicationContext(gac: GenericApplicationContext): GenericApplicationContext {
      return spyk(gac) {
        every { publishEvent(any()) } just runs
      }
    }

    @Bean
    @Primary
    fun workQueueProcessor(): WorkQueueProcessor = mockk {
      every { queueCodeEventForProcessing(any()) } just Runs
      every { queueArtifactForProcessing(any()) } just Runs
    }
  }

  @Autowired
  lateinit var eventPublisher: ApplicationEventPublisher

  @Autowired
  lateinit var workQueueProcessor: WorkQueueProcessor

  private class Fixture {
    private val objectMapper = configuredObjectMapper()

    val disguisedCodeEvent = EchoArtifactEvent(
      eventName = "test",
      payload = ArtifactPublishedEvent(
        artifacts = listOf(
          PublishedArtifact(
            name = "master:953910b24a776eceab03d4dcae8ac050b2e0b668",
            type = "pr_opened",
            reference = "https://stash/projects/ORG/repos/myrepo/commits/953910b24a776eceab03d4dcae8ac050b2e0b668",
            version = "953910b24a776eceab03d4dcae8ac050b2e0b668",
            provenance = "https://stash/projects/ORG/repos/myrepo/commits/953910b24a776eceab03d4dcae8ac050b2e0b668",
            metadata = mapOf(
              "repoKey" to "stash/org/myrepo",
              "prId" to "11494",
              "sha" to  "953910b24a776eceab03d4dcae8ac050b2e0b668",
              "branch" to "master",
              "prBranch" to "feature/branch",
              "targetBranch" to "master",
              "originalPayload" to mapOf(
                "causedBy" to mapOf(
                  "email" to "keel@keel"
                ),
                "target" to mapOf(
                  "projectKey" to "org",
                  "repoName" to "myrepo"
                )
              )
            )
          )
        )
      )
    )

    val disguisedBuildEvent = EchoArtifactEvent(
      eventName = "test",
      payload = ArtifactPublishedEvent(
        artifacts = listOf(
          PublishedArtifact(
            name = "See image.properties",
            type = "docker",
            reference = "image.properties",
            version = "See image.properties",
            metadata = mapOf(
              "eventType" to "BUILD"
            )
          )
        )
      )
    )

    var response: ResultActions? = null

    fun postArtifact(event: EchoArtifactEvent, mvc: MockMvc) {
      //clearMocks(workQueueProcessor, answers = false)
      val request = MockMvcRequestBuilders.post("/artifacts/events")
        .contentType(MediaType.APPLICATION_JSON_VALUE)
        .content(objectMapper.writeValueAsString(event))

      response = mvc.perform(request)
    }
  }
  
  fun tests() = rootContext<Fixture> {
    fixture {
      Fixture()
    }

    context("a code event disguised as an artifact event is received") {
      before {
        clearMocks(workQueueProcessor, answers = false)
        postArtifact(disguisedCodeEvent, mvc)
      }

      test("request succeeds") {
        response!!.andExpect(status().isAccepted)
      }

      test("event is properly translated and queued as code event") {
        verify(exactly = 1) {
          workQueueProcessor.queueCodeEventForProcessing(any())
        }
      }

      test("original artifact event is not queued") {
        verify(exactly = 0) {
          workQueueProcessor.queueArtifactForProcessing(any())
        }
      }
    }

    context("a Docker build event disguised as an artifact event is received") {
      before {
        clearMocks(workQueueProcessor, answers = false)
        postArtifact(disguisedBuildEvent, mvc)
      }

      test("request succeeds") {
        response!!.andExpect(status().isAccepted)
      }

      test("artifact is queued") {
        val queuedArtifact = slot<PublishedArtifact>()

        verify(exactly = 1) {
          workQueueProcessor.queueArtifactForProcessing(capture(queuedArtifact))
        }

        expectThat(queuedArtifact.captured) {
          get { type }.isEqualTo(DOCKER)
        }
      }
    }
  }
}
