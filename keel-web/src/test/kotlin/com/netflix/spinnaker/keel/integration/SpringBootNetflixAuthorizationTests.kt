package com.netflix.spinnaker.keel.integration

import com.netflix.authorizable.context.AuthorizationMode.ENFORCE_SINGLE_MOST_DIRECT_ID
import com.netflix.gandalf.agent.AuthorizationClient
import com.netflix.gandalf.agent.protogen.AuthorizationResponse
import com.netflix.spinnaker.keel.KeelApplication
import com.netflix.spinnaker.keel.integration.SpringBootNetflixAuthorizationTests.DummyController
import com.netflix.springboot.sso.test.EnableSsoTest
import com.netflix.springboot.sso.test.WithSsoUser
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.MOCK
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders
import org.springframework.test.web.servlet.result.MockMvcResultMatchers
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

/**
 * Verifies our custom integration with SBN for authorization.
 */
@SpringBootTest(
  webEnvironment = MOCK,
  classes = [ KeelApplication::class, DummyController::class ],
  properties = [
    "netflix.sso.direct-caller-authorization-filter.enabled=true"
  ]
)
@AutoConfigureMockMvc
@EnableSsoTest
internal class SpringBootNetflixAuthorizationTests
@Autowired constructor(val mvc: MockMvc) {
  @MockkBean
  lateinit var authorizationClient: AuthorizationClient

  @RestController
  class DummyController {
    @GetMapping("/dummy")
    fun doNothing() {}
  }

  @BeforeEach
  fun setup() {
    every {
      authorizationClient.isAuthorized(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        ENFORCE_SINGLE_MOST_DIRECT_ID,
        "REST",
        any(),
        any()
      )
    } returns AuthorizationResponse.newBuilder().setAllowed(true).build()
  }

  @Test
  fun `returns 403 with no authenticated user`() {
    val request = MockMvcRequestBuilders.get("/dummy")
      .accept(MediaType.APPLICATION_JSON_VALUE)
    mvc
      .perform(request.secure(true))
      .andExpect(MockMvcResultMatchers.status().isForbidden)
  }

  @Test
  @WithSsoUser(name = "alice")
  fun `successfully authorizes request with SSO user`() {
    val request = MockMvcRequestBuilders.get("/dummy")
      .accept(MediaType.APPLICATION_JSON_VALUE)
    mvc
      .perform(request.secure(true))
      .andExpect(MockMvcResultMatchers.status().isOk)
  }
}
