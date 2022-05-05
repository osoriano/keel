package com.netflix.spinnaker.keel.dgs

import com.netflix.spinnaker.keel.actuation.ExecutionSummary
import com.netflix.spinnaker.keel.actuation.RolloutStatus
import com.netflix.spinnaker.keel.actuation.RolloutTargetWithStatus
import com.netflix.spinnaker.keel.actuation.Stage
import com.netflix.spinnaker.keel.api.AccountAwareLocations
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.DeployableResourceSpec
import com.netflix.spinnaker.keel.api.Locatable
import com.netflix.spinnaker.keel.api.Monikered
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.TaskStatus
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.migration.ApplicationMigrationStatus
import com.netflix.spinnaker.keel.api.migration.PipelineArtifact
import com.netflix.spinnaker.keel.api.migration.PipelineConstraint
import com.netflix.spinnaker.keel.api.migration.PipelineResource
import com.netflix.spinnaker.keel.api.migration.MigrationPipeline
import com.netflix.spinnaker.keel.api.migration.PipelineStatus
import com.netflix.spinnaker.keel.bakery.diff.PackageDiff
import com.netflix.spinnaker.keel.core.api.ActuationPlan
import com.netflix.spinnaker.keel.core.api.PinType
import com.netflix.spinnaker.keel.core.api.ResourceAction.CREATE
import com.netflix.spinnaker.keel.core.api.ResourceAction.NONE
import com.netflix.spinnaker.keel.core.api.ResourceAction.UPDATE
import com.netflix.spinnaker.keel.graphql.types.MD_ActuationPlan
import com.netflix.spinnaker.keel.graphql.types.MD_ActuationPlanStatus
import com.netflix.spinnaker.keel.graphql.types.MD_Artifact
import com.netflix.spinnaker.keel.graphql.types.MD_ArtifactSpec
import com.netflix.spinnaker.keel.graphql.types.MD_ArtifactVersionInEnvironment
import com.netflix.spinnaker.keel.graphql.types.MD_CommitInfo
import com.netflix.spinnaker.keel.graphql.types.MD_ConstraintSpec
import com.netflix.spinnaker.keel.graphql.types.MD_DeployLocation
import com.netflix.spinnaker.keel.graphql.types.MD_DeployTarget
import com.netflix.spinnaker.keel.graphql.types.MD_EnvironmentPlan
import com.netflix.spinnaker.keel.graphql.types.MD_EventLevel
import com.netflix.spinnaker.keel.graphql.types.MD_ExecutionSummary
import com.netflix.spinnaker.keel.graphql.types.MD_GitMetadata
import com.netflix.spinnaker.keel.graphql.types.MD_Location
import com.netflix.spinnaker.keel.graphql.types.MD_Migration
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationStatus.BLOCKED
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationStatus.COMPLETED
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationStatus.NOT_READY
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationStatus.PR_CREATED
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationStatus.READY_TO_START
import com.netflix.spinnaker.keel.graphql.types.MD_Moniker
import com.netflix.spinnaker.keel.graphql.types.MD_Notification
import com.netflix.spinnaker.keel.graphql.types.MD_PackageAndVersion
import com.netflix.spinnaker.keel.graphql.types.MD_PackageAndVersionChange
import com.netflix.spinnaker.keel.graphql.types.MD_PackageDiff
import com.netflix.spinnaker.keel.graphql.types.MD_PausedInfo
import com.netflix.spinnaker.keel.graphql.types.MD_PinType
import com.netflix.spinnaker.keel.graphql.types.MD_Pipeline
import com.netflix.spinnaker.keel.graphql.types.MD_PipelineStatus.EXPORTED
import com.netflix.spinnaker.keel.graphql.types.MD_PipelineStatus.PROCESSED
import com.netflix.spinnaker.keel.graphql.types.MD_PullRequest
import com.netflix.spinnaker.keel.graphql.types.MD_Resource
import com.netflix.spinnaker.keel.graphql.types.MD_ResourceAction
import com.netflix.spinnaker.keel.graphql.types.MD_ResourceActuationState
import com.netflix.spinnaker.keel.graphql.types.MD_ResourceActuationStatus
import com.netflix.spinnaker.keel.graphql.types.MD_ResourcePlan
import com.netflix.spinnaker.keel.graphql.types.MD_ResourceSpec
import com.netflix.spinnaker.keel.graphql.types.MD_ResourceTask
import com.netflix.spinnaker.keel.graphql.types.MD_RolloutTargetStatus
import com.netflix.spinnaker.keel.graphql.types.MD_StageDetail
import com.netflix.spinnaker.keel.notifications.DismissibleNotification
import com.netflix.spinnaker.keel.pause.Pause
import com.netflix.spinnaker.keel.persistence.TaskForResource
import com.netflix.spinnaker.keel.services.ResourceActuationState

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
    displayName = displayName,
    moniker = getMdMoniker(),
    location = (spec as? Locatable<*>)?.let {
      val account = when (val locations = it.locations) {
        is AccountAwareLocations -> locations.account
        else -> null
      }
      MD_Location(account = account, regions = it.locations.regions.map { r -> r.name })
    },
    // TODO: replace this with isRedeployable indicating whether the resource can *currently*
    //  be redeployed (e.g. there are no verifications running, it's not locked, etc.)
    isDeployable = spec is DeployableResourceSpec
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

val TaskForResource.shortName: String
  get() = name.substringBefore("[").trim()

fun TaskForResource.toDgs(): MD_ResourceTask {
  return MD_ResourceTask(
    id = id,
    name = shortName,
    fullName = name,
    running = endedAt == null
  )
}

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

/**
 * We convert the complicated orca status to a more simple status for the UI to display
 */
fun TaskStatus.toDgs(): MD_RolloutTargetStatus =
  when (this) {
    TaskStatus.NOT_STARTED -> MD_RolloutTargetStatus.NOT_STARTED
    TaskStatus.RUNNING -> MD_RolloutTargetStatus.RUNNING
    TaskStatus.PAUSED -> MD_RolloutTargetStatus.FAILED
    TaskStatus.SUSPENDED -> MD_RolloutTargetStatus.FAILED
    TaskStatus.SUCCEEDED -> MD_RolloutTargetStatus.SUCCEEDED
    TaskStatus.FAILED_CONTINUE -> MD_RolloutTargetStatus.FAILED
    TaskStatus.TERMINAL -> MD_RolloutTargetStatus.FAILED
    TaskStatus.CANCELED -> MD_RolloutTargetStatus.FAILED
    TaskStatus.REDIRECT -> MD_RolloutTargetStatus.FAILED
    TaskStatus.STOPPED -> MD_RolloutTargetStatus.FAILED
    TaskStatus.BUFFERED -> MD_RolloutTargetStatus.NOT_STARTED
    TaskStatus.SKIPPED -> MD_RolloutTargetStatus.FAILED
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

fun PinType.toDgs(): MD_PinType =
  when(this) {
    PinType.ROLLBACK -> MD_PinType.ROLLBACK
    PinType.LOCK -> MD_PinType.LOCK
  }

fun ResourceActuationState.toDgs(): MD_ResourceActuationState =
  MD_ResourceActuationState(
    resourceId = resourceId,
    status = MD_ResourceActuationStatus.valueOf(status.name),
    reason = reason,
    event = eventMessage,
    errors = errors
  )

fun List<MigrationPipeline>?.toDgs(): List<MD_Pipeline>? =
  this?.map { pipeline->
    MD_Pipeline(
      id = pipeline.id,
      name = pipeline.name,
      status = when (pipeline.status) {
        PipelineStatus.EXPORTED -> EXPORTED
        else ->  PROCESSED
      },
      reason = pipeline.reason?.name,
      shape = pipeline.shape,
      resources = pipeline.resources.toResources(),
      artifacts = pipeline.artifacts.toArtifacts(),
      constraints = pipeline.constraints.toConstraints()
      )
  }?.toList()

private fun Set<PipelineConstraint>?.toConstraints(): List<MD_ConstraintSpec>? {
  return this?.map { constraint ->
    MD_ConstraintSpec(
      id = constraint.type,
      type = constraint.type,
      spec = constraint
    )
  }?.toList()
}

private fun Set<PipelineResource>?.toResources(): List<MD_ResourceSpec>? {
  return this?.map { resource ->
    MD_ResourceSpec(
      id = resource.id,
      kind = resource.kind,
      spec = resource
    )
  }?.toList()
}

private fun Set<PipelineArtifact>?.toArtifacts(): List<MD_ArtifactSpec>? {
  return this?.map { artifact ->
    MD_ArtifactSpec(
      type = artifact.type,
      id = artifact.name,
      spec = artifact
    )
  }?.toList()
}

fun ApplicationMigrationStatus.toDgs(appName: String) = MD_Migration(
  id = "migration-$appName",
  status = when {
    alreadyManaged -> COMPLETED
    prLink != null -> PR_CREATED
    isBlocked -> BLOCKED
    !isMigratable -> NOT_READY
    isMigratable -> READY_TO_START
    else -> NOT_READY
  },
  deliveryConfig = deliveryConfig,
  prLink = prLink,
  pipelines = pipelines.toDgs()
)

fun ActuationPlan.toDgs(completed: Boolean) =
  MD_ActuationPlan(
    id = "$application-actuationPlan",
    status = when (completed) {
      true -> MD_ActuationPlanStatus.COMPLETED
      false -> MD_ActuationPlanStatus.PENDING
    },
    environmentPlans = environmentPlans.map { envPlan ->
      MD_EnvironmentPlan(
        id = "$application-${envPlan.environment}-plan",
        environment = envPlan.environment,
        resourcePlans = envPlan.resourcePlans.map { resourcePlan ->
          MD_ResourcePlan(
            id = resourcePlan.resourceId,
            action = when(resourcePlan.action) {
              NONE -> MD_ResourceAction.NONE
              CREATE -> MD_ResourceAction.CREATE
              UPDATE -> MD_ResourceAction.UPDATE
            },
            diff = resourcePlan.diff
          )
        }
      )
    }
  )
