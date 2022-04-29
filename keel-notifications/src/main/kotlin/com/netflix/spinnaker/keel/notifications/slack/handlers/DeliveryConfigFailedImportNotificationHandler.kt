package com.netflix.spinnaker.keel.notifications.slack.handlers

import com.netflix.spinnaker.keel.api.NotificationDisplay
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.notifications.NotificationType.DELIVERY_CONFIG_IMPORT_FAILED
import com.netflix.spinnaker.keel.notifications.slack.SlackFailedToImportConfigNotification
import com.netflix.spinnaker.keel.notifications.slack.SlackService
import com.slack.api.model.block.LayoutBlock
import com.slack.api.model.kotlin_extension.block.withBlocks
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Sends notifications if a delivery config failed to imported (similar to the UI error)
 */
@Component
class DeliveryConfigFailedImportNotificationHandler(
  private val slackService: SlackService,
  private val gitDataGenerator: GitDataGenerator,
) : SlackNotificationHandler<SlackFailedToImportConfigNotification> {
  override val supportedTypes = listOf(DELIVERY_CONFIG_IMPORT_FAILED)

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  private fun SlackFailedToImportConfigNotification.headerText(): String {
    return "Failed to import delivery config for [$application]"
  }

  private fun SlackFailedToImportConfigNotification.toBlocks(): List<LayoutBlock> {

    val headerText = "Failed to import delivery config from branch ${gitMetadata.branch}"
    return withBlocks {

      section {
        markdownText(":X: *$headerText for ${gitDataGenerator.linkedCommitTitleSnippet(gitMetadata, application)}*")
      }

      section {
        val hash = gitMetadata.commitInfo!!.sha
        markdownText("\nReason: ${gitDataGenerator.formatMessage(gitMetadata.copy(commitInfo = Commit(message = reason, sha = hash)))}")
        gitDataGenerator.buildMoreInfoButton(this, hash, reason, "See details", "FULL_REASON_MODAL")
      }

      section {
        gitDataGenerator.generateScmInfo(this, application, gitMetadata, null)
      }
    }
  }

  override fun sendMessage(
    notification: SlackFailedToImportConfigNotification,
    channel: String,
    notificationDisplay: NotificationDisplay
  ) {
    with(notification) {
      log.debug("Sending failed to import config notification for application ${notification.application}")

      slackService.sendSlackNotification(
        channel,
        notification.toBlocks(),
        application = application,
        type = supportedTypes,
        fallbackText = headerText()
      )
    }
  }
}
