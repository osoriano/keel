package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.stash.BuildState
import com.netflix.spinnaker.keel.api.stash.BuildState.SUCCESSFUL
import com.netflix.spinnaker.keel.api.stash.BuildState.UNKNOWN

val PublishedArtifact.isArtifactEvent: Boolean
  get() = metadata["rocketEventType"] == "ARTIFACT"

val PublishedArtifact.isBuildEvent: Boolean
  get() = metadata["rocketEventType"] == "BUILD"

val PublishedArtifact.buildController: String?
  get() = metadata["controllerName"] as? String

val PublishedArtifact.buildJob: String?
  get() = metadata["jobName"] as? String

val PublishedArtifact.buildStatus: BuildState
  get() = (metadata["buildDetail"] as? Map<String, Any>)?.get("result")
    ?.let { BuildState.valueOf(it.toString()) }
    ?: UNKNOWN

val PublishedArtifact.isIncompleteDockerArtifact: Boolean
  get() = isBuildEvent && buildStatus == SUCCESSFUL && type == DOCKER && name.contains(".properties")
