package com.netflix.spinnaker.keel.rest

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.Verification
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus.OVERRIDE_PASS
import com.netflix.spinnaker.keel.api.constraints.UpdatedConstraintStatus
import com.netflix.spinnaker.keel.auth.AuthorizationSupport
import com.netflix.spinnaker.keel.auth.AuthorizationSupport.TargetEntity.APPLICATION
import com.netflix.spinnaker.keel.auth.PermissionLevel.READ
import com.netflix.spinnaker.keel.auth.PermissionLevel.WRITE
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.services.ApplicationService
import com.netflix.spinnaker.keel.test.deliveryConfig
import com.netflix.spinnaker.time.MutableClock
import com.netflix.springboot.sso.test.EnableSsoTest
import com.netflix.springboot.sso.test.WithSsoUser
import com.ninjasquad.springmockk.MockkBean
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.MOCK
import org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import strikt.api.expectThat
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.isNotEmpty

@SpringBootTest(webEnvironment = MOCK)
@AutoConfigureMockMvc
@EnableSsoTest
@WithSsoUser(name = "alice")
internal class ApplicationControllerTests
@Autowired constructor(
  val mvc: MockMvc,
  val jsonMapper: ObjectMapper
) : JUnit5Minutests {

  @MockkBean
  lateinit var authorizationSupport: AuthorizationSupport

  @MockkBean
  lateinit var applicationService: ApplicationService

  @MockkBean
  lateinit var actuationPauser: ActuationPauser
  val clock = MutableClock()

  companion object {
    const val application = "fnord"
    const val user = "keel@keel.io"
    val deliveryConfig = deliveryConfig()
  }

  val payloadWithLongComment =
    """{
        |  "version": "master-h22.0e0310f",
        |  "reference": "my-artifact",
        |  "targetEnvironment": "testing",
        |  "comment": "Bacon ipsum dolor amet turducken prosciutto shoulder ground round hamburger, flank frankfurter rump ham hock sirloin leberkas meatloaf shankle landjaeger pig.  Shoulder shankle doner ball tip burgdoggen kevin alcatra bresaola.  Leberkas alcatra cow, sausage picanha chislic tongue hamburger turkey tail chicken flank.",
        |}"""
      .trimMargin()

  fun tests() = rootContext {
    after {
      clearAllMocks()
    }

    context("application with delivery config exists") {
      before {
        val verification = mockk<Verification>()
        val staging = Environment(name="staging", verifyWith=listOf(verification))

        authorizationSupport.allowAll()
        every { applicationService.hasManagedResources(application) } returns true
        every { applicationService.getResourceSummariesFor(application) } returns emptyList()
        every { applicationService.getDeliveryConfig(application) } returns deliveryConfig
      }

      test("can get delivery config") {
        val request = get("/application/$application/config")
          .accept(APPLICATION_JSON_VALUE)
        val result = mvc
          .perform(request.secure(true))
          .andExpect(status().isOk)
          .andDo { print(it.response.contentAsString) }
          .andReturn()
        val response = jsonMapper.readValue<Map<String, Any>>(result.response.contentAsString)
        expectThat(response).isNotEmpty()
      }

      context("with un-paused application") {
        before {
          every { actuationPauser.applicationIsPaused(application) } returns false
        }

        test("can get basic summary by application") {
          val request = get("/application/$application")
            .accept(APPLICATION_JSON_VALUE)
          mvc
            .perform(request.secure(true))
            .andExpect(status().isOk)
            .andExpect(
              content().json(
                """
              {
                "applicationPaused":false,
                "hasManagedResources":true
              }
                """.trimIndent()
              )
            )
        }

        test("can get resource summaries") {
          val request =
            get("/application/$application?entities=resources")
              .accept(APPLICATION_JSON_VALUE)
          val result = mvc
            .perform(request.secure(true))
            .andExpect(status().isOk)
            .andDo { println(it.response.contentAsString) }
            .andReturn()
          val response = jsonMapper.readValue<Map<String, Any>>(result.response.contentAsString)
          expectThat(response.keys)
            .containsExactlyInAnyOrder(
              "applicationPaused",
              "hasManagedResources",
              "resources",
            )
        }
      }

      context("with paused application") {
        before {
          every {
            actuationPauser.applicationIsPaused(application)
          } returns true
        }

        test("reflects application paused status in basic summary") {
          val request = get("/application/$application")
            .accept(APPLICATION_JSON_VALUE)
          mvc
            .perform(request.secure(true))
            .andExpect(status().isOk)
            .andExpect(
              content().json(
                """
              {
                "applicationPaused":true,
                "hasManagedResources":true
              }
                """.trimIndent()
              )
            )
        }
      }
    }

    context("application is not managed") {
      before {
        every { applicationService.hasManagedResources(any()) } returns false
        every { actuationPauser.applicationIsPaused(any()) } returns false
        authorizationSupport.allowAll()
      }

      test("API returns gracefully") {
        val request = get("/application/bananas")
          .accept(APPLICATION_JSON_VALUE)
        mvc
          .perform(request.secure(true))
          .andExpect(status().isOk)
          .andExpect(
            content().json(
              """
              {
                "hasManagedResources":false
              }
              """.trimIndent()
            )
          )
      }
    }

    context("API permission checks") {
      context("GET /application/fnord") {
        context("with no READ access to application") {
          before {
            authorizationSupport.denyApplicationAccess(READ, APPLICATION)
            authorizationSupport.allowCloudAccountAccess(READ, APPLICATION)
          }
          test("request is forbidden") {
            val request = get("/application/fnord")
              .accept(APPLICATION_JSON_VALUE)
              .header("X-SPINNAKER-USER", user)

            mvc.perform(request.secure(true)).andExpect(status().isForbidden)
          }
        }
        context("with no READ access to cloud account") {
          before {
            authorizationSupport.denyCloudAccountAccess(READ, APPLICATION)
            authorizationSupport.allowApplicationAccess(READ, APPLICATION)
          }
          test("request is forbidden") {
            val request = get("/application/fnord")
              .accept(APPLICATION_JSON_VALUE)
              .header("X-SPINNAKER-USER", user)

            mvc.perform(request.secure(true)).andExpect(status().isForbidden)
          }
        }
      }
      context("POST /application/fnord/environment/prod/constraint") {
        context("with no WRITE access to application") {
          before {
            authorizationSupport.denyApplicationAccess(WRITE, APPLICATION)
            authorizationSupport.allowServiceAccountAccess()
          }
          test("request is forbidden") {
            val request = post("/application/fnord/environment/prod/constraint").addData(
              jsonMapper,
              UpdatedConstraintStatus("manual-judgement", "prod", "deb", OVERRIDE_PASS)
            )
              .accept(APPLICATION_JSON_VALUE)
              .header("X-SPINNAKER-USER", user)

            mvc.perform(request.secure(true)).andExpect(status().isForbidden)
          }
        }
        context("with no access to service account") {
          before {
            authorizationSupport.denyServiceAccountAccess()
            authorizationSupport.allowApplicationAccess(WRITE, APPLICATION)
          }
          test("request is forbidden") {
            val request = post("/application/fnord/environment/prod/constraint").addData(
              jsonMapper,
              UpdatedConstraintStatus("manual-judgement", "prod", "deb", OVERRIDE_PASS)
            )
              .accept(APPLICATION_JSON_VALUE)
              .header("X-SPINNAKER-USER", user)

            mvc.perform(request.secure(true)).andExpect(status().isForbidden)
          }
        }
      }
      context("POST /application/fnord/pause") {
        context("with no WRITE access to application") {
          before {
            authorizationSupport.denyApplicationAccess(WRITE, APPLICATION)
          }
          test("request is forbidden") {
            val request = post("/application/fnord/pause")
              .accept(APPLICATION_JSON_VALUE)
              .header("X-SPINNAKER-USER", user)

            mvc.perform(request.secure(true)).andExpect(status().isForbidden)
          }
        }
      }
      context("DELETE /application/fnord/pause") {
        context("with no WRITE access to application") {
          before {
            authorizationSupport.denyApplicationAccess(WRITE, APPLICATION)
            authorizationSupport.allowServiceAccountAccess()
          }
          test("request is forbidden") {
            val request = delete("/application/fnord/pause")
              .accept(APPLICATION_JSON_VALUE)
              .header("X-SPINNAKER-USER", user)

            mvc.perform(request.secure(true)).andExpect(status().isForbidden)
          }
        }
        context("with no access to service account") {
          before {
            authorizationSupport.denyServiceAccountAccess()
            authorizationSupport.allowApplicationAccess(WRITE, APPLICATION)
          }
          test("request is forbidden") {
            val request = delete("/application/fnord/pause")
              .accept(APPLICATION_JSON_VALUE)
              .header("X-SPINNAKER-USER", user)

            mvc.perform(request.secure(true)).andExpect(status().isForbidden)
          }
        }
      }
      context("GET /application/fnord/config") {
        context("with no READ access to application") {
          before {
            authorizationSupport.denyApplicationAccess(READ, APPLICATION)
          }
          test("request is forbidden") {
            val request = get("/application/fnord/config")
              .accept(APPLICATION_JSON_VALUE)
              .header("X-SPINNAKER-USER", user)

            mvc.perform(request.secure(true)).andExpect(status().isForbidden)
          }
        }
      }
      context("DELETE /application/fnord/config") {
        context("with no WRITE access to application") {
          before {
            authorizationSupport.denyApplicationAccess(WRITE, APPLICATION)
          }
          test("request is forbidden") {
            val request = delete("/application/fnord/config")
              .accept(APPLICATION_JSON_VALUE)
              .header("X-SPINNAKER-USER", "keel@keel.io")

            mvc.perform(request.secure(true)).andExpect(status().isForbidden)
          }
        }
      }
    }
  }
}
