package com.netflix.spinnaker.keel.api.artifacts

/**
 * The git metadata of an artifact.
 */
data class GitMetadata(
  val commit: String, // commit hash, can be short or long sha
  val author: String? = null,
  val project: String? = null, // the project name, like SPKR
  val branch: String? = null,
  val repo: Repo? = null,
  val pullRequest: PullRequest? = null,
  val commitInfo: Commit? = null
) {
  fun incompleteMetadata(): Boolean =
    repo == null || project == null || branch == null
}

data class Repo(
  val name: String? = null,
  val link: String? = null
)

data class PullRequest(
  val number: String? = null,
  val url: String? = null
)

data class Commit(
  val sha: String,
  val link: String? = null,
  val message: String? = null
)

/**
 *  @return A copy of this [GitMetadata] object if not null, with the specified [commit] and [branch], or a new object with those values.
 */
fun GitMetadata?.copyOrCreate(commit: String, branch: String): GitMetadata {
  return this?.copy(commit = commit, branch = branch, commitInfo = Commit(sha = commit)) ?:
  GitMetadata(commit, branch = branch, commitInfo = Commit(sha = commit))
}
