package com.netflix.spinnaker.keel.igor.model

/**
 * The details of a (Jenkins) build, as reported by Rocket.
 */
data class BuildDetail(
  val buildEngine: String,
  val buildId: String,
  val buildDisplayName: String,
  val buildDescription: String,
  val buildUrl: String,
  val buildNumber: Int,
  val result: BuildState,
  val startedAt: Long,
  val queuedAt: Long,
  val completedAt: Long,
  val commitId: String,
  val artifacts: List<String> = emptyList(),
  val logs: List<String> = emptyList(),
  val reports: List<String> = emptyList(),
)
