package com.netflix.spinnaker.keel.notifications.slack.handlers

import com.netflix.spinnaker.keel.api.NotificationDisplay
import com.netflix.spinnaker.keel.notifications.NotificationType
import com.netflix.spinnaker.keel.notifications.slack.SlackService
import com.netflix.spinnaker.keel.notifications.slack.SlackUnpinnedNotification
import com.slack.api.model.block.LayoutBlock
import com.slack.api.model.kotlin_extension.block.withBlocks
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Sends notification when unpinning an artifact
 */
@Component
class UnpinnedNotificationHandler(
  private val slackService: SlackService,
  private val gitDataGenerator: GitDataGenerator
) : SlackNotificationHandler<SlackUnpinnedNotification> {

  override val supportedTypes = listOf(NotificationType.ARTIFACT_UNPINNED)
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  private fun SlackUnpinnedNotification.headerText(): String {
    return "[$application] lock removed from ${targetEnvironment.lowercase()}"
  }

  private fun SlackUnpinnedNotification.toBlocks(): List<LayoutBlock> =
    withBlocks {
      val previouslyPinned = pinnedArtifact
        ?.let {
          val link = gitDataGenerator.generateArtifactUrl(application, originalPin.artifact.reference, it.version)
          "<$link|#${it.buildNumber ?: it.version}>"
        } ?: originalPin.version

      val header = ":unlock2: *${gitDataGenerator.linkedApp(application)} lock on build $previouslyPinned removed from ${gitDataGenerator.toCode(targetEnvironment)}*"

      val unpinner = slackService.getUsernameByEmail(user)
      val isPinnedVersionAlreadyDeployed = latestApprovedArtifactVersion?.version == originalPin.version
      var text = "$unpinner unlocked ${gitDataGenerator.toCode(targetEnvironment)}"

      if (latestApprovedArtifactVersion != null) {
        val link = gitDataGenerator.generateArtifactUrl(application, originalPin.artifact.reference, latestApprovedArtifactVersion.version)
        //if latest version == pinned version, show a different message
        text += if (isPinnedVersionAlreadyDeployed) {
          " The latest version is already deployed, and new versions can be deployed now."
        } else {
          ", <$link|#${latestApprovedArtifactVersion.buildNumber ?: latestApprovedArtifactVersion.version}> will start deploying shortly"
        }
      }

      section {
        markdownText(header + "\n\n" + text)
      }

      if (latestApprovedArtifactVersion != null && !isPinnedVersionAlreadyDeployed) {
        gitDataGenerator.buildCommitSectionWithButton(this, latestApprovedArtifactVersion.gitMetadata)
      }

      pinnedArtifact?.gitMetadata?.let { gitMetadata ->
        section {
          gitDataGenerator.generateScmInfo(this, application, gitMetadata, pinnedArtifact)
        }
      }

      val pinner: String = originalPin.pinnedBy?.let {
        slackService.getUsernameByEmail(originalPin.pinnedBy!!)
      } ?: "Somebody"

      context {
        elements {
          markdownText("$pinner originally locked on " +
            "<!date^${originalPin.pinnedAt!!.epochSecond}^{date_num} {time_secs}|fallback-text-include-PST>" +
            ": \"${originalPin.comment}\"")
        }
      }
    }

  override fun sendMessage(
    notification: SlackUnpinnedNotification,
    channel: String,
    notificationDisplay: NotificationDisplay
  ) {
    with(notification) {
      log.debug("Sending unpinned artifact notification for application $application")

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
