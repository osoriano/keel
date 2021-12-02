package com.netflix.spinnaker.keel.api.migration

/**
 * An object containing the data needed in order to open a PR for an application with a config file in Igor.
 */
data class MigrationCommitData (
  val fileContents: String,
  val commitMessage: String,
  val branchName: String,
  val prTitle: String,
  val prDescription: String,
  val filePath: String,
  val reviewers: Set<String>
  )
