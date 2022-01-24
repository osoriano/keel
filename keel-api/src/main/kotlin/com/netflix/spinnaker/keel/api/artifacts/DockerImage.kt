package com.netflix.spinnaker.keel.api.artifacts

data class DockerImage(
  val account: String,
  val repository: String,
  val tag: String,
  val digest: String?,
  val buildNumber: String? = null,
  val commitId: String? = null,
  val prCommitId: String? = null,
  val branch: String? = null,
  val date: String? = null,
)
