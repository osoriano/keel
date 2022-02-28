package com.netflix.spinnaker.keel.notifications.slack.handlers

import com.netflix.spinnaker.config.BaseUrlConfig
import com.netflix.spinnaker.keel.api.NotificationDisplay
import com.netflix.spinnaker.keel.notifications.NotificationType
import com.netflix.spinnaker.keel.notifications.slack.SlackMigrationNotification
import com.netflix.spinnaker.keel.notifications.slack.SlackService
import com.slack.api.model.block.LayoutBlock
import com.slack.api.model.kotlin_extension.block.withBlocks
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.stereotype.Component

/**
 * Sends notification when a migration is ready
 */
@Component
@EnableConfigurationProperties(BaseUrlConfig::class)
class MigrationNotificationHandler(
  private val slackService: SlackService,
  private val baseUrlConfig: BaseUrlConfig,
) : SlackNotificationHandler<SlackMigrationNotification> {

  override val supportedTypes = listOf(NotificationType.MIGRATION_READY_NOTIFICATION)

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  private fun SlackMigrationNotification.headerText(): String {
    return "[$application] is ready for an upgrade"
  }

  private fun SlackMigrationNotification.toBlocks(): List<LayoutBlock> {
    val headerText = "*:managed: :rainbow4: ${linkedApp(baseUrlConfig, application)} is ready for an upgrade*"

    val text = "\nYour app is ready for an upgrade to *Managed Delivery: a simpler way to deploy your code.* It’s safe, reversible, and takes less than 5 minutes. \n" +
      ":clapper: <https://drive.google.com/file/d/1rnxHVC-dGM0JU3Xv2TtRlE0GJI4PQo8g/view|Watch a 1 minute demo>\n\n" +
      "*Why upgrade?*\n Easily track your commits, interactive Slack notifications, preview environments, awesome integration with DGS, simple rollback and many other perks.\n\n"

    return withBlocks {
      section {
        markdownText(headerText + "\n" + text)
      }
      actions {
          button {
            style("primary")
            text("Upgrade now", emoji = true)
            url(envUrl(baseUrlConfig, application))
            // action id will be consisted by 3 sections with ":" between them to keep it consistent
            actionId("button:url:migration-link")
          }
      }
    }
  }

  override fun sendMessage(
    notification: SlackMigrationNotification,
    channel: String,
    notificationDisplay: NotificationDisplay
  ) {
    log.debug("Sending migration ready notification for application ${notification.application}")

    with(notification) {
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
