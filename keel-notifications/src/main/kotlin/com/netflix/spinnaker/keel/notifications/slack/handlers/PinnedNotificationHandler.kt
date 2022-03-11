package com.netflix.spinnaker.keel.notifications.slack.handlers

import com.netflix.spinnaker.keel.api.NotificationDisplay
import com.netflix.spinnaker.keel.notifications.NotificationType
import com.netflix.spinnaker.keel.notifications.slack.SlackPinnedNotification
import com.netflix.spinnaker.keel.notifications.slack.SlackService
import com.slack.api.model.block.LayoutBlock
import com.slack.api.model.kotlin_extension.block.withBlocks
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Sends notification when pinning an artifact
 */
@Component
class PinnedNotificationHandler(
  private val slackService: SlackService,
  private val gitDataGenerator: GitDataGenerator
) : SlackNotificationHandler<SlackPinnedNotification> {

  override val supportedTypes = listOf(NotificationType.ARTIFACT_PINNED)
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  private fun SlackPinnedNotification.headerText(): String {
    return "[$application]'s ${pin.targetEnvironment.lowercase()} environment was locked to build ${pinnedArtifact.buildNumber ?: pinnedArtifact.version}"
  }

  private fun SlackPinnedNotification.toBlocks(): List<LayoutBlock> {
    val username = pin.pinnedBy?.let { slackService.getUsernameByEmail(it) }

    return withBlocks {
      section {
        markdownText(":lock: *${gitDataGenerator.linkedApp(application)}'s ${gitDataGenerator.toCode(pin.targetEnvironment)} environment was locked by $username to build ${gitDataGenerator.generateBuildUrlWithFallback(pinnedArtifact, application)}*")
      }

      section {
        markdownText("locked on <!date^${time.epochSecond}^{date_num} {time_secs}|fallback-text-include-PST>: \"${pin.comment}\"")
      }

      pinnedArtifact.gitMetadata?.let { gitMetadata ->
        section {
          gitDataGenerator.generateScmInfo(this, application, gitMetadata, pinnedArtifact)
        }
      }
    }
  }

  override fun sendMessage(
    notification: SlackPinnedNotification,
    channel: String,
    notificationDisplay: NotificationDisplay
  ) {
    with(notification) {
      log.debug("Sending pinned artifact notification for application $application")

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
