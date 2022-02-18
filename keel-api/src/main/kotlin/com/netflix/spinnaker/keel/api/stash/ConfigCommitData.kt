package com.netflix.spinnaker.keel.api.stash

data class ConfigCommitData(
  val commitId: String,
  val isExists: Boolean = false
)

data class Commit(
  val id: String,
  val displayId: String
)
