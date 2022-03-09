package com.netflix.spinnaker.keel.slack

import com.netflix.spinnaker.config.SlackConfiguration
import com.netflix.spinnaker.keel.activation.ApplicationUp
import com.netflix.spinnaker.keel.notifications.slack.callbacks.FullMessageModalCallbackHandler
import com.netflix.spinnaker.keel.notifications.slack.callbacks.ManualJudgmentCallbackHandler
import com.slack.api.bolt.App
import com.slack.api.bolt.context.builtin.ActionContext
import com.slack.api.bolt.request.builtin.BlockActionRequest
import com.slack.api.bolt.response.Response
import com.slack.api.bolt.socket_mode.SocketModeApp
import com.slack.api.socket_mode.SocketModeClient.Backend.Tyrus
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component


/**
 * Handles callbacks from Slack for interactive notifications.
 *
 * We use the Slack Bolt library (https://github.com/slackapi/java-slack-sdk/), which has native support for
 * handling such callbacks.
 */
@Component
class SlackCallbackHandler(
  private val slackApp: App,
  private val slackConfig: SlackConfiguration,
  private val manualJudgementCallbackHandler: ManualJudgmentCallbackHandler,
  private val fullMessageModalCallbackHandler: FullMessageModalCallbackHandler,
) {

  companion object {
    private const val MANUAL_JUDGEMENT_ACTION = "MANUAL_JUDGMENT"
    private const val SHOW_FULL_COMMIT_ACTION = "FULL_COMMIT_MODAL"
    private const val SHOW_FAILURE_ACTION = "FULL_REASON_MODAL"
    private const val SHOW_DIFF_ACTION = "mj-diff-link"
    private const val MIGRATION_READY_ACTION = "migration-link"

    // The pattern here should match the action id field in the actual button.
    // For example, for manual judgment notifications: constraintId:OVERRIDE_PASS:MANUAL_JUDGMENT
    private val ACTION_ID_PATTERN = "^(\\w+):(\\w+):(\\w+)".toPattern()
  }

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  @EventListener(ApplicationUp::class)
  fun init(event: ApplicationUp) {
    slackApp.blockAction(ACTION_ID_PATTERN) { req: BlockActionRequest, ctx: ActionContext ->
      handleBlockAction(req, ctx)
    }

    if (slackConfig.socketMode) {
      try {
        val socketModeApp = SocketModeApp(
          slackConfig.appToken,
          Tyrus,
          slackApp
        )
        // Each instance will start a websocket. We can open up to 10. Slack will send each callback to a single socket.
        // https://api.slack.com/apis/connections/socket-implement#connections
        socketModeApp.startAsync()
        log.debug("Started Slack integration in websocket mode.")
      } catch (e: Exception) {
        log.error("Error starting Slack websocket integration: $e", e)
      }
    }
  }

  /**
   * Handles the incoming block action request from Slack.
   */
  fun handleBlockAction(
    request: BlockActionRequest,
    context: ActionContext
  ): Response {
    log.debug("Received Slack callback for action: ${request.actionId}, triggerId: ${context.triggerId}")

    when (request.notificationType) {
      MANUAL_JUDGEMENT_ACTION -> {
        log.debug(logMessage("manual judgment button clicked", request))
        manualJudgementCallbackHandler.respondToButton(request, context)
      }
      SHOW_FULL_COMMIT_ACTION -> {
        log.debug(logMessage("show full commit button clicked", request))
        fullMessageModalCallbackHandler.openModal(request, context)
      }
      //reusing the same modal for commit message and failure reason
      SHOW_FAILURE_ACTION -> {
        log.debug(logMessage("show full failure reason button clicked", request))
        fullMessageModalCallbackHandler.openModal(request, context)
      }
      SHOW_DIFF_ACTION -> {
        log.debug(logMessage("'see changes' button clicked", request))
      }
      MIGRATION_READY_ACTION -> {
        log.debug(logMessage("'start migration' button clicked", request))
      }
      else -> {
        log.warn(logMessage("Unrecognized action", request))
      }
    }
    // we always need to acknowledge the button within 3 seconds
    // TODO: should we move this to before the handler calls, since handling is asynchronous anyway?
    return context.ack()
  }

  fun logMessage(what: String, req: BlockActionRequest) =
    "[slack interaction] $what by ${req.payload?.user?.username} (${req.payload?.user?.id}) " +
      "in channel ${req.payload?.channel?.name} (${req.payload?.channel?.id})"

  private val BlockActionRequest.actionId: String
    get() = payload.actions.first().actionId

  // action id is consistent of 3 parts, where the last part is the type
  private val BlockActionRequest.notificationType
    get() = actionId.split(":").last()
}
