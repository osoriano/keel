package com.netflix.spinnaker.keel.notifications

import com.netflix.spinnaker.keel.api.UID
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.Repo
import com.netflix.spinnaker.keel.events.EventLevel
import com.netflix.spinnaker.keel.events.EventLevel.ERROR
import java.time.Instant

/**
 * A [DismissibleNotification] that indicates a delivery config failed to import.
 */
data class DeliveryConfigImportFailed(
  override val triggeredAt: Instant,
  override val application: String,
  override val environment: String? = null,
  val reason: String,
  val repoType: String,
  val projectKey: String,
  val authorEmail: String?,
  val commitHash: String? = null,
  val repoSlug: String,
  override val branch: String,
  override val link: String? = null,
  override var uid: UID? = null,
) : DismissibleNotification() {
  override val level: EventLevel = ERROR
  override val triggeredBy: String = "Managed Delivery"
  override val message: String
    get() {
      val commitText = if (commitHash != null) {
        " [${commitHash.short}]"
      } else {
        ""
      }
      return "Failed to import delivery config from branch $branch$commitText. Reason: $reason"
    }
}

private val String.short: String
  get() = substring(0, 7)

fun DeliveryConfigImportFailed.gitMetadata(): GitMetadata {
    return GitMetadata(
      commitInfo = Commit(
        sha = commitHash,
        link = link
      ),
      branch = branch,
      project = projectKey,
      repo = Repo(
        name = repoSlug
      ),
      commit = commitHash ?: "",
      author = authorEmail
    )
}
