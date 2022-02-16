package com.netflix.spinnaker.keel.api.stash

data class PullRequest(
  val title: String,
  val description: String,
  val fromRef: Ref,
  val toRef: Ref,
  var reviewers: Set<Reviewer>? = emptySet(),
  val links: PullRequestLinks? = null
)

data class Reviewer(
  val user: User
)

data class User(
  val name: String
)

data class PullRequestLinks(
  val self: List<StashLinks>
)

data class StashLinks(
  val href: String
)
