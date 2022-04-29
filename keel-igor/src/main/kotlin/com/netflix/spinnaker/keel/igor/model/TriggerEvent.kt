package com.netflix.spinnaker.keel.igor.model

data class TriggerEvent(
  val repoKey: String,
  val source: CommitDetails,
  val target: CommitDetails,
  val pullRequest: PullRequest?
)

data class Author(
  val name: String,
  val email: String
)

data class CommitDetails(
  val projectKey: String,
  val repoName: String,
  val branchName: String,
  val sha: String,
  val message: String,
  val url: String,
  val author: Author,
  val date: String
)

data class PullRequest(
  val id: String,
  val url: String,
  val title: String
)
