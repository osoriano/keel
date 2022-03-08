package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.keel.notifications.slack.callbacks.FullMessageModalCallbackHandler
import com.netflix.spinnaker.keel.notifications.slack.callbacks.ManualJudgmentCallbackHandler
import com.slack.api.bolt.App
import com.slack.api.bolt.context.builtin.ActionContext
import com.slack.api.bolt.request.builtin.BlockActionRequest
import com.slack.api.bolt.servlet.SlackAppServlet
import org.slf4j.LoggerFactory
import javax.servlet.annotation.WebServlet


/**
 * A [WebServlet] that handles callbacks from Slack for interactive notifications.
 *
 * We use the Slack Bolt library (https://github.com/slackapi/java-slack-sdk/), which has native support for
 * handling such callbacks.
 */
@WebServlet(
  name = "SlackAppServlet",
  urlPatterns = ["/slack/notifications/callbacks"]
)
class SlackAppServlet(
  slackApp: App,
  private val manualJudgementCallbackHandler: ManualJudgmentCallbackHandler,
  private val fullMessageModalCallbackHandler: FullMessageModalCallbackHandler,
) : SlackAppServlet(slackApp) {

  companion object {
    private const val MANUAL_JUDGEMENT_ACTION = "MANUAL_JUDGMENT"
    private const val SHOW_FULL_COMMIT_ACTION = "FULL_COMMIT_MODAL"
    private const val SHOW_FAILURE_ACTION = "FULL_REASON_MODAL"
    private const val SHOW_DIFF_ACTION = "mj-diff-link"
    private const val MIGRATION_READY_ACTION = "migration-link"
  }

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  init {
    // The pattern here should match the action id field in the actual button.
    // For example, for manual judgment notifications: constraintId:OVERRIDE_PASS:MANUAL_JUDGMENT
    val actionIdPattern = "^(\\w+):(\\w+):(\\w+)".toPattern()
    slackApp.blockAction(actionIdPattern) { req: BlockActionRequest, ctx: ActionContext ->
      log.debug("Received Slack callback for action: ${req.actionId}, triggerId: ${ctx.triggerId}")

      when (req.notificationType) {
        MANUAL_JUDGEMENT_ACTION -> {
          log.debug(logMessage("manual judgment button clicked", req))
          manualJudgementCallbackHandler.respondToButton(req, ctx)
        }
        SHOW_FULL_COMMIT_ACTION -> {
          log.debug(logMessage("show full commit button clicked", req))
          fullMessageModalCallbackHandler.openModal(req, ctx)
        }
        //reusing the same modal for commit message and failure reason
        SHOW_FAILURE_ACTION -> {
          log.debug(logMessage("show full failure reason button clicked", req))
          fullMessageModalCallbackHandler.openModal(req, ctx)
        }
        SHOW_DIFF_ACTION -> {
          log.debug(logMessage("'see changes' button clicked", req))
        }
        MIGRATION_READY_ACTION -> {
          log.debug(logMessage("'start migration' button clicked", req))
        }
        else -> {
          log.warn(logMessage("Unrecognized action", req))
        }
      }
      // we always need to acknowledge the button within 3 seconds
      // TODO: should we move this to before the handler calls, since handling is asynchronous anyway?
      ctx.ack()
    }
  }

  fun logMessage(what: String, req: BlockActionRequest) =
    "[slack interaction] $what by ${req.payload?.user?.username} (${req.payload?.user?.id}) " +
      "in channel ${req.payload?.channel?.name} (${req.payload?.channel?.id})"

  private val BlockActionRequest.actionId: String
    get() = payload.actions.first().actionId

  //action id is consistent of 3 parts, where the last part is the type
  private val BlockActionRequest.notificationType
    get() = actionId.split(":").last()
}
