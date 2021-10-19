package com.netflix.spinnaker.keel.dgs

import com.netflix.spinnaker.keel.api.AccountAwareLocations
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Locatable
import com.netflix.spinnaker.keel.api.Monikered
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.SimpleLocations
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import com.netflix.spinnaker.keel.api.TaskStatus
import com.netflix.spinnaker.keel.actuation.ExecutionSummary
import com.netflix.spinnaker.keel.actuation.RolloutStatus
import com.netflix.spinnaker.keel.actuation.RolloutTargetWithStatus
import com.netflix.spinnaker.keel.actuation.Stage
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.bakery.diff.PackageDiff
import com.netflix.spinnaker.keel.graphql.types.MD_Artifact
import com.netflix.spinnaker.keel.graphql.types.MD_ArtifactVersionInEnvironment
import com.netflix.spinnaker.keel.graphql.types.MD_CommitInfo
import com.netflix.spinnaker.keel.graphql.types.MD_DeployLocation
import com.netflix.spinnaker.keel.graphql.types.MD_DeployTarget
import com.netflix.spinnaker.keel.graphql.types.MD_EventLevel
import com.netflix.spinnaker.keel.graphql.types.MD_ExecutionSummary
import com.netflix.spinnaker.keel.graphql.types.MD_GitMetadata
import com.netflix.spinnaker.keel.graphql.types.MD_Location
import com.netflix.spinnaker.keel.graphql.types.MD_Moniker
import com.netflix.spinnaker.keel.graphql.types.MD_Notification
import com.netflix.spinnaker.keel.graphql.types.MD_PackageAndVersion
import com.netflix.spinnaker.keel.graphql.types.MD_PackageAndVersionChange
import com.netflix.spinnaker.keel.graphql.types.MD_PackageDiff
import com.netflix.spinnaker.keel.graphql.types.MD_PausedInfo
import com.netflix.spinnaker.keel.graphql.types.MD_PullRequest
import com.netflix.spinnaker.keel.graphql.types.MD_Resource
import com.netflix.spinnaker.keel.graphql.types.MD_ResourceTask
import com.netflix.spinnaker.keel.graphql.types.MD_RolloutTargetStatus
import com.netflix.spinnaker.keel.graphql.types.MD_StageDetail
import com.netflix.spinnaker.keel.graphql.types.MD_TaskStatus
import com.netflix.spinnaker.keel.notifications.DismissibleNotification
import com.netflix.spinnaker.keel.pause.Pause
import com.netflix.spinnaker.keel.persistence.TaskForResource


fun GitMetadata.toDgs(): MD_GitMetadata =
  MD_GitMetadata(
    commit = commit,
    author = author,
    project = project,
    branch = branch,
    repoName = repo?.name,
    pullRequest = if (pullRequest != null) {
      MD_PullRequest(
        number = pullRequest?.number,
        link = pullRequest?.url
      )
    } else null,
    commitInfo = if (commitInfo != null) {
      MD_CommitInfo(
        sha = commitInfo?.sha,
        link = commitInfo?.link,
        message = commitInfo?.message
      )
    } else null
  )


fun Resource<*>.toDgs(config: DeliveryConfig, environmentName: String): MD_Resource =
  MD_Resource(
    id = id,
    kind = kind.toString(),
    artifact = findAssociatedArtifact(config)?.let { artifact ->
      MD_Artifact(
        id = "$environmentName-${artifact.reference}",
        environment = environmentName,
        name = artifact.name,
        type = artifact.type,
        reference = artifact.reference
      )
    },
    displayName = spec.displayName,
    moniker = getMdMoniker(),
    location = (spec as? Locatable<*>)?.let {
      val account = when (val locations = it.locations) {
        is AccountAwareLocations -> locations.account
        is SubnetAwareLocations -> locations.account
        is SimpleLocations -> locations.account
        else -> null
      }
      MD_Location(account = account, regions = it.locations.regions.map { r -> r.name })
    }
  )

fun Resource<*>.getMdMoniker(): MD_Moniker? {
  with(spec) {
    return if (this is Monikered) {
      MD_Moniker(
        app = moniker.app,
        stack = moniker.stack,
        detail = moniker.detail,
      )
    } else {
      null
    }
  }
}

fun PackageDiff.toDgs() =
  MD_PackageDiff(
    added = added.map { (pkg, version) -> MD_PackageAndVersion(pkg, version) },
    removed = removed.map { (pkg, version) -> MD_PackageAndVersion(pkg, version) },
    changed = changed.map { (pkg, versions) -> MD_PackageAndVersionChange(pkg, versions.old, versions.new) }
  )

fun Pause.toDgsPaused(): MD_PausedInfo =
  MD_PausedInfo(
    id = "$scope-$name-pause",
    by = pausedBy,
    at = pausedAt,
    comment = comment
  )

fun DismissibleNotification.toDgs() =
  MD_Notification(
    id = uid?.toString() ?: error("Can't convert application event with missing UID: $this"),
    level = MD_EventLevel.valueOf(level.name),
    message = message,
    isActive = isActive,
    triggeredAt = triggeredAt,
    triggeredBy = triggeredBy,
    environment = environment,
    link = link,
    dismissedAt = dismissedAt,
    dismissedBy = dismissedBy
  )

fun ExecutionSummary.toDgs() =
  MD_ExecutionSummary(
    id = "$id:summary",
    status = status.toDgs(),
    currentStage = currentStage?.toDgs(),
    stages = stages.map { it.toDgs() },
    deployTargets = deployTargets.map { it.toDgs() },
    error = error
  )

fun TaskForResource.toDgs() =
  MD_ResourceTask(
    id = id,
    name = name,
    running = endedAt == null
  )

fun RolloutTargetWithStatus.toDgs() =
  MD_DeployTarget(
    cloudProvider = rolloutTarget.cloudProvider,
    location = MD_DeployLocation(rolloutTarget.location.account, rolloutTarget.location.region, rolloutTarget.location.sublocations),
    status = status.toDgs()
  )

fun RolloutStatus.toDgs() =
  when (this) {
    RolloutStatus.NOT_STARTED -> MD_RolloutTargetStatus.NOT_STARTED
    RolloutStatus.RUNNING -> MD_RolloutTargetStatus.RUNNING
    RolloutStatus.SUCCEEDED -> MD_RolloutTargetStatus.SUCCEEDED
    RolloutStatus.FAILED ->MD_RolloutTargetStatus.FAILED
  }

fun Stage.toDgs() =
  MD_StageDetail(
    id = id,
    type = type,
    name = name,
    startTime = startTime,
    endTime = endTime,
    status = status.toDgs(),
    refId = refId,
    requisiteStageRefIds = requisiteStageRefIds,
  )

fun TaskStatus.toDgs(): MD_TaskStatus =
  when (this) {
    TaskStatus.NOT_STARTED -> MD_TaskStatus.NOT_STARTED
    TaskStatus.RUNNING -> MD_TaskStatus.RUNNING
    TaskStatus.PAUSED -> MD_TaskStatus.PAUSED
    TaskStatus.SUSPENDED -> MD_TaskStatus.SUSPENDED
    TaskStatus.SUCCEEDED -> MD_TaskStatus.SUCCEEDED
    TaskStatus.FAILED_CONTINUE -> MD_TaskStatus.FAILED_CONTINUE
    TaskStatus.TERMINAL -> MD_TaskStatus.TERMINAL
    TaskStatus.CANCELED -> MD_TaskStatus.CANCELED
    TaskStatus.REDIRECT -> MD_TaskStatus.REDIRECT
    TaskStatus.STOPPED -> MD_TaskStatus.STOPPED
    TaskStatus.BUFFERED -> MD_TaskStatus.BUFFERED
    TaskStatus.SKIPPED -> MD_TaskStatus.SKIPPED
}

fun PublishedArtifact.toDgs(environmentName: String) =
  MD_ArtifactVersionInEnvironment(
    id = "latest-approved-${environmentName}-${reference}-${version}",
    version = version,
    buildNumber = buildNumber,
    createdAt = createdAt,
    gitMetadata = if (gitMetadata == null) {
      null
    } else {
      gitMetadata?.toDgs()
    },
    environment = environmentName,
    reference = reference,
  )
