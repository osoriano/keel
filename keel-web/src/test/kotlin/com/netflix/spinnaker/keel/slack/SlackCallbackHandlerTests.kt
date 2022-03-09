package com.netflix.spinnaker.keel.slack

import com.netflix.spinnaker.config.SlackConfiguration
import com.netflix.spinnaker.keel.activation.ApplicationUp
import com.netflix.spinnaker.keel.notifications.slack.callbacks.FullMessageModalCallbackHandler
import com.netflix.spinnaker.keel.notifications.slack.callbacks.ManualJudgmentCallbackHandler
import com.slack.api.app_backend.interactive_components.payload.BlockActionPayload
import com.slack.api.bolt.App
import com.slack.api.bolt.context.builtin.ActionContext
import com.slack.api.bolt.request.RequestType.BlockAction
import com.slack.api.bolt.request.builtin.BlockActionRequest
import com.slack.api.bolt.response.Response
import com.slack.api.util.json.GsonFactory
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.regex.Pattern

class SlackCallbackHandlerTests {
  private val manualJudgementHandler: ManualJudgmentCallbackHandler = mockk()
  private val fullMessageModalCallbackHandler: FullMessageModalCallbackHandler = mockk()
  private val slackJsonFactory = GsonFactory.createSnakeCase()
  private val slackConfig = SlackConfiguration().apply { socketMode = false }

  private val slackApp = spyk(
    object : App() {
      fun handle(request: BlockActionRequest) =
        runHandler(request)
    }
  )

  private val actionContext: ActionContext = mockk {
    every { requestUserId } returns "user"
    every { triggerId } returns "trigger"
    every { ack() } returns Response.ok()
  }

  private val subject = SlackCallbackHandler(slackApp, slackConfig, manualJudgementHandler, fullMessageModalCallbackHandler)

  @BeforeEach
  fun setup() {
    subject.init(ApplicationUp)

    every {
      manualJudgementHandler.respondToButton(any(), any())
    } just runs

    every {
      fullMessageModalCallbackHandler.openModal(any(), any())
    } just runs

    every {
      slackApp.blockAction(any<Pattern>(), any())
    } returns slackApp
  }

  @Test
  fun `delegates handling of manual judgement callback`() {
    val request = buildBlockActionRequest("manual-judgement-payload")

    slackApp.handle(request)

    verify {
      manualJudgementHandler.respondToButton(any(), any())
    }
  }

  @Test
  fun `delegates handling of commit modal callback`() {
    val request = buildBlockActionRequest("show-commit-payload")

    slackApp.handle(request)

    verify {
      fullMessageModalCallbackHandler.openModal(any(), any())
    }
  }

  @Test
  fun `delegates handling of failure reason to commit modal callback`() {
    val request = buildBlockActionRequest("show-failure-reason-payload")

    slackApp.handle(request)

    verify {
      fullMessageModalCallbackHandler.openModal(any(), any())
    }
  }

  private fun buildBlockActionRequest(payloadName: String): BlockActionRequest {
    return javaClass.getResource("/slack/$payloadName.json").readText().let {
      val blocks = slackJsonFactory.fromJson(it, BlockActionPayload::class.java)
      mockk {
        every { payload } returns blocks
        every { requestType } returns BlockAction
        every { context } returns actionContext
      }
    }
  }
}
