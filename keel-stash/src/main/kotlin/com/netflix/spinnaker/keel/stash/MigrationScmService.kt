package com.netflix.spinnaker.keel.stash

import com.netflix.spinnaker.keel.api.StashBridge
import com.netflix.spinnaker.keel.api.migration.MigrationCommitData
import com.netflix.spinnaker.keel.api.stash.BranchResponse
import com.netflix.spinnaker.keel.api.stash.BuildResult
import com.netflix.spinnaker.keel.api.stash.BuildState
import com.netflix.spinnaker.keel.api.stash.ConfigCommitData
import com.netflix.spinnaker.keel.api.stash.Project
import com.netflix.spinnaker.keel.api.stash.PullRequest
import com.netflix.spinnaker.keel.api.stash.Ref
import com.netflix.spinnaker.keel.api.stash.Repo
import com.netflix.spinnaker.keel.api.stash.Reviewer
import com.netflix.spinnaker.keel.api.stash.User
import com.netflix.spinnaker.kork.exceptions.SystemException
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import retrofit2.HttpException

@Component
class MigrationScmService(
  private val stashService: StashService
) : StashBridge {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  companion object {
    const val MD_PROJECT_KEY = "MDMIG"
    const val COMMIT_MESSAGE = "Your first delivery config [skip ci]"
    const val PR_TITLE = "Upgrade to Managed Delivery"
    const val BRANCH_NAME = "md-migration"
    const val CONFIG_PATH = ".netflix/spinnaker.yml"
  }

  override suspend fun createCommitAndPrFromConfig(
    migrationCommitData: MigrationCommitData
  ): String? {
    val projectKey = migrationCommitData.projectKey
    val repoSlug = migrationCommitData.repoSlug
    try {
      // 1. Create a fork in MD_PROJECT_KEY with the customer's repo
      migrationCommitData.createFork(Project(MD_PROJECT_KEY))
      // 2. Get the default branch
      val defaultBranch = stashService.getDefaultBranch(projectKey, repoSlug)
      // 3. create a commit
      val commit = migrationCommitData.createCommit(defaultBranch)
      // 4. store successful build status for the newly created commit which will cause to skip the build for that commit
      try {
        migrationCommitData.storeSuccessfulBuildStatus(commit.commitId)
      } catch (ex: Exception) {
        log.debug(
          "Caught an exception while storing a build status for project $projectKey:$repoSlug", ex)
        throw StashException("failed to store a build status for repo $repoSlug")
      }
      // 5. open a PR based on the previously created branch, from MD_PROJECT_KEY to the customer's repo
      return migrationCommitData.createPullRequest(
        defaultBranch,
        Project(MD_PROJECT_KEY),
        commit.isExists)

    } catch (ex: Exception) {
      log.debug(
        "Caught an exception while calling stash for $projectKey:$repoSlug", ex)
      throw StashException("failed to create a pull request for $projectKey:$repoSlug; Exception is ${ex.message}")
    }
  }

  override suspend fun deleteFork(repoSlug: String) {
    log.debug("starting to delete repo $repoSlug fork from $MD_PROJECT_KEY project")
    stashService.deleteFork(MD_PROJECT_KEY, repoSlug)
    log.debug("repo $repoSlug fork was deleted successfully from $MD_PROJECT_KEY project")
  }

  private fun MigrationCommitData.storeSuccessfulBuildStatus(commitId: String) {
    runBlocking {
      stashService.storeBuildStatus(projectKey, repoSlug, commitId, BuildResult(
        key = "Skipping build check for MD migration",
        state = BuildState.SUCCESSFUL,
        url = "https://manuals.netflix.net/view/spinnaker/mkdocs/master/managed-delivery/landing/"
      ))
    }
  }

  fun MigrationCommitData.createPullRequest(
    defaultBranch: BranchResponse,
    mdProject: Project,
    isDuplicate: Boolean
  ): String? {

    val customerProject = Project(projectKey)
    val customerRepo = Repo(repoSlug, customerProject)
    val mdRepo = Repo(repoSlug, mdProject)
    val fromRef = Ref("refs/heads/$BRANCH_NAME", mdRepo)
    val toRef = Ref(defaultBranch.id, customerRepo)
    val stashReviewers: Set<Reviewer> = reviewers(user)
    var prDescription = getPrDescription(user)

    // if the config was there before, add a warning to the beginning of the PR description
    if (isDuplicate) {
      prDescription = getDuplicatePrDescription(user = user)
    }

    val request = PullRequest(PR_TITLE, prDescription, fromRef, toRef, stashReviewers)
    return try {
      runBlocking {
        stashService.openPullRequest(projectKey, repoSlug, request).links?.self?.get(0)?.href
      }
    } catch (ex: Exception) {
      //Stash returns a conflict error when the reviewers are invalid, so we'll retry the request with an empty set
      abortUnlessConflictError(ex)
      log.debug("Reviewers $stashReviewers are invalid. Setting empty reviewers set for pull request for project $projectKey:$repoSlug", ex)
      request.reviewers = emptySet()
      runBlocking {
        stashService.openPullRequest(projectKey, repoSlug, request).links?.self?.get(0)?.href
      }
    }
  }

  private fun reviewers(user: String): Set<Reviewer> {
    val reviewer = if (user.contains('@')) {
      user.substringBefore('@')
      //we don't want to fail the request or throw an exception if the user is invalid
    } else ""

    val reviewers: Set<User> = setOf(User(reviewer))

    val stashReviewers: Set<Reviewer> = reviewers.map {
      Reviewer(it)
    }.toSet()
    return stashReviewers
  }

  private fun MigrationCommitData.createCommit(
    defaultBranch: BranchResponse
  ): ConfigCommitData {
    return try {
      val commit = runBlocking {
        stashService.addCommit(
          projectKey = MD_PROJECT_KEY,
          repositorySlug = repoSlug,
          path = CONFIG_PATH,
          content = fileContents,
          branch = BRANCH_NAME,
          message = COMMIT_MESSAGE,
          sourceBranch = defaultBranch.displayId,
          sourceCommitId = null
        )
      }
      ConfigCommitData(commitId = commit.id, isExists = false)
    } catch (ex: Exception) {
      abortUnlessConflictError(ex)
      //in case the file exists, we need to set the ref of the default branch as the source commit id, and then call again to add the commit
      //try again to add the commit after updating the latest commit id
      return try {
        log.debug("Retrying to create a commit for: $repoSlug with an addition of sourceCommitId")
        val commit = runBlocking {
          stashService.addCommit(
            projectKey = MD_PROJECT_KEY,
            repositorySlug = repoSlug,
            path = CONFIG_PATH,
            content = fileContents,
            branch = BRANCH_NAME,
            message = COMMIT_MESSAGE,
            sourceBranch = defaultBranch.displayId,
            sourceCommitId = defaultBranch.id
          )
        }
        ConfigCommitData(commitId = commit.id, isExists = true)
      } catch (ex: Exception) {
        abortUnlessConflictError(ex)
        //at this point, we are getting another conflict exception
        log.debug("Retrying to create a commit for repo $repoSlug failed. Is it possible that a branch named $BRANCH_NAME exists in the original repo or in the forked repo?")
        throw StashException("branch $BRANCH_NAME already exists in repo $repoSlug")
      }
    }
  }

  private fun MigrationCommitData.createFork(mdProject: Project) {
    runBlocking {
      try {
        val repo = Repo(slug = repoSlug, project = mdProject)
        stashService.createFork(projectKey, repoSlug, repo)
      } catch (ex: Exception) {
        abortUnlessConflictError(ex)
        log.debug("A fork for repo: $repoSlug was already exists.")
      }
    }
  }

  //This function is checking if stash returned conflict 409 error, and throw the exception otherwise
  fun abortUnlessConflictError(ex: Exception) {
    if (ex !is HttpException || ex.code() != HttpStatus.CONFLICT.value()) {
      throw StashException(ex.message)
    }
  }

  class StashException(message: String?) : SystemException("Caught an exception while calling stash: $message")

}
