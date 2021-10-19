package com.netflix.spinnaker.keel.notifications.slack.callbacks

import com.netflix.spinnaker.config.SlackConfiguration
import com.netflix.spinnaker.keel.notifications.slack.handlers.GitDataGenerator
import com.slack.api.app_backend.interactive_components.payload.BlockActionPayload
import com.slack.api.bolt.context.builtin.ActionContext
import com.slack.api.bolt.request.builtin.BlockActionRequest
import com.slack.api.model.view.View
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * A handler that builds a modal to be displayed in Slack when the message is longer than what will fit in a notification.
 */
@Component
class FullMessageModalCallbackHandler(
  private val gitDataGenerator: GitDataGenerator,
  private val slackConfiguration: SlackConfiguration
) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  fun openModal(req: BlockActionRequest, ctx: ActionContext) {
    val modal: View = buildView(req.payload)
    val response = ctx.client().viewsOpen { r ->
      r
        .token(slackConfiguration.token)
        .triggerId(req.payload.triggerId)
        .view(modal)
    }
    if (!response.isOk) {
      log.error("Failed to send slack callback: {}", response)
    }
  }

  fun buildView(slackCallbackResponse: BlockActionPayload): View {
    val message = slackCallbackResponse.getMessage
    val hash = slackCallbackResponse.getHash
    val text = slackCallbackResponse.getText()
    return gitDataGenerator.buildFullMessageModal(message = message, hash = hash, text = text)
  }

  val BlockActionPayload.getMessage: String
    get() = actions.first().value

  val BlockActionPayload.getHash: String
    get() = actions.first().actionId.split(":")[1]

  //options here are commit view or reason view
  fun BlockActionPayload.getText(): String{
    val text = actions.first().actionId.split(":")[2]
    return when {
      "REASON" in text -> "reason-message"
      else -> "commit-message"
    }
  }

}
