package com.netflix.spinnaker.keel.api.migration

/**
 * An object containing the data needed in order to open a PR for an application with a config file in Igor.
 */
data class MigrationCommitData (
  val fileContents: String,
  val user: String,
  val projectKey: String,
  val repoSlug: String
  )
