package com.netflix.spinnaker.keel.stash

import com.netflix.spinnaker.keel.api.migration.MigrationCommitData
import com.netflix.spinnaker.keel.api.stash.BranchResponse
import com.netflix.spinnaker.keel.api.stash.Commit
import com.netflix.spinnaker.keel.api.stash.Project
import com.netflix.spinnaker.keel.api.stash.PullRequest
import com.netflix.spinnaker.keel.api.stash.PullRequestLinks
import com.netflix.spinnaker.keel.api.stash.Ref
import com.netflix.spinnaker.keel.api.stash.Repo
import com.netflix.spinnaker.keel.api.stash.Reviewer
import com.netflix.spinnaker.keel.api.stash.StashLinks
import com.netflix.spinnaker.keel.api.stash.User
import com.netflix.spinnaker.keel.stash.MigrationScmService.Companion.CONFIG_PATH
import com.netflix.spinnaker.keel.stash.MigrationScmService.Companion.MD_PROJECT_KEY
import com.netflix.spinnaker.keel.stash.MigrationScmService.Companion.PR_TITLE
import io.mockk.Runs
import io.mockk.coEvery
import io.mockk.just
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.jupiter.api.Test
import retrofit2.HttpException
import retrofit2.Response
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo

internal class MigrationScmServiceTests {

  private val stashService = mockk<StashService>()
  private val subject = MigrationScmService(stashService)
  private val repoSlug = "keel"
  private val projectKey = "spkr"
  private val user = "waffle@"
  private val migrationCommitData = MigrationCommitData(
    repoSlug = repoSlug,
    projectKey = projectKey,
    user = user,
    fileContents = "this is a delivery config"
  )
  private val customerProject = Project(projectKey)
  private val customerRepo = Repo(repoSlug, customerProject)
  private val mdProject = Project(MD_PROJECT_KEY)
  private val mdRepo = Repo(repoSlug, mdProject)
  private val defaultBranch = BranchResponse(id = "/refs/heads/main", displayId = "main")
  private val commit = Commit(id = "12345", displayId = "12345")
  private val pullRequest = PullRequest(title = PR_TITLE,
              description =  getPrDescription(user),
    fromRef = Ref("refs/heads/${MigrationScmService.BRANCH_NAME}", mdRepo),
    toRef = Ref(defaultBranch.id, customerRepo),
    reviewers = setOf(Reviewer(User("waffle")))
  )

  val pullRequestResponse = pullRequest.copy(
    links = PullRequestLinks(
      self = listOf(
        StashLinks(
          href = "link-to-pull-request"
        )
      )
    )
  )

  val RETROFIT_INTERNAL_ERROR = HttpException(
    Response.error<Any>(500, "".toResponseBody("application/json".toMediaTypeOrNull()))
  )

  val RETROFIT_CONFLICT_ERROR = HttpException(
    Response.error<Any>(409, "".toResponseBody("application/json".toMediaTypeOrNull()))
  )

  @Test
  fun `creating fork throws an exception`() {
    coEvery {  stashService.createFork(projectKey, repoSlug, any()) } throws RETROFIT_INTERNAL_ERROR

    runBlocking {
      expectThrows<MigrationScmService.StashException> {
        subject.createCommitAndPrFromConfig(migrationCommitData)
      }
    }
  }


  @Test
  fun `happy flow - open a pull request`() {
    coEvery {  stashService.createFork(projectKey, repoSlug, any()) } just Runs
    coEvery { stashService.getDefaultBranch(projectKey, repoSlug) } returns defaultBranch
    coEvery { stashService.addCommit(MD_PROJECT_KEY, repoSlug, CONFIG_PATH, any(), any(), any(), any(), any()) } returns commit
    coEvery { stashService.storeBuildStatus(projectKey, repoSlug, commit.id, any()) } returns Response.success(null)
    coEvery { stashService.openPullRequest(projectKey, repoSlug, pullRequest) } returns pullRequest.copy(
      links = PullRequestLinks(
        self = listOf(
          StashLinks(
            href = "link-to-pull-request"
          )
        )
      )
    )
    val link = runBlocking {
        subject.createCommitAndPrFromConfig(migrationCommitData)
    }
    expectThat(link).isEqualTo("link-to-pull-request")
  }

  @Test
  fun `pull request is created even if the fork is there`() {
    coEvery {  stashService.createFork(projectKey, repoSlug, any()) } throws RETROFIT_CONFLICT_ERROR
    coEvery { stashService.getDefaultBranch(projectKey, repoSlug) } returns defaultBranch
    coEvery { stashService.addCommit(MD_PROJECT_KEY, repoSlug, CONFIG_PATH, any(), any(), any(), any(), any()) } returns commit
    coEvery { stashService.storeBuildStatus(projectKey, repoSlug, commit.id, any()) } returns Response.success(null)
    coEvery { stashService.openPullRequest(projectKey, repoSlug, pullRequest) } returns pullRequestResponse
    val link = runBlocking {
      subject.createCommitAndPrFromConfig(migrationCommitData)
    }
    expectThat(link).isEqualTo("link-to-pull-request")
  }

  @Test
  fun `pull request is created even if the file was there originally`() {
    coEvery {  stashService.createFork(projectKey, repoSlug, any()) } just Runs
    coEvery { stashService.getDefaultBranch(projectKey, repoSlug) } returns defaultBranch
    coEvery { stashService.addCommit(MD_PROJECT_KEY, repoSlug, CONFIG_PATH, any(), any(), any(), any(), any()) } throws RETROFIT_CONFLICT_ERROR
    coEvery { stashService.addCommit(MD_PROJECT_KEY, repoSlug, CONFIG_PATH, any(), any(), any(), any(), defaultBranch.id) } returns commit
    coEvery { stashService.storeBuildStatus(projectKey, repoSlug, commit.id, any()) } returns Response.success(null)
    coEvery { stashService.openPullRequest(projectKey, repoSlug, pullRequest.copy(
      description = getDuplicatePrDescription(user)
    )) } returns pullRequestResponse
    val link = runBlocking {
      subject.createCommitAndPrFromConfig(migrationCommitData)
    }
    expectThat(link).isEqualTo("link-to-pull-request")
  }

  @Test
  fun `pull request is created even if the reviewer list is invalid`() {
    coEvery {  stashService.createFork(projectKey, repoSlug, any()) } just Runs
    coEvery { stashService.getDefaultBranch(projectKey, repoSlug) } returns defaultBranch
    coEvery { stashService.addCommit(MD_PROJECT_KEY, repoSlug, CONFIG_PATH, any(), any(), any(), any(), any()) } returns commit
    coEvery { stashService.storeBuildStatus(projectKey, repoSlug, commit.id, any()) } returns Response.success(null)
    coEvery { stashService.openPullRequest(projectKey, repoSlug, pullRequest) } throws RETROFIT_CONFLICT_ERROR
    coEvery { stashService.openPullRequest(projectKey, repoSlug, pullRequest.copy(reviewers = emptySet())) } returns pullRequestResponse
    val link = runBlocking {
      subject.createCommitAndPrFromConfig(migrationCommitData)
    }
    expectThat(link).isEqualTo("link-to-pull-request")
  }

  @Test
  fun `pull request is not created because md-migration branch is already in their repo`() {
    coEvery {  stashService.createFork(projectKey, repoSlug, any()) } just Runs
    coEvery { stashService.getDefaultBranch(projectKey, repoSlug) } returns defaultBranch
    coEvery { stashService.addCommit(MD_PROJECT_KEY, repoSlug, CONFIG_PATH, any(), any(), any(), any(), any()) } throws RETROFIT_CONFLICT_ERROR
    coEvery { stashService.addCommit(MD_PROJECT_KEY, repoSlug, CONFIG_PATH, any(), any(), any(), any(), defaultBranch.id) } throws RETROFIT_CONFLICT_ERROR
    runBlocking {
      expectThrows<MigrationScmService.StashException> {
        subject.createCommitAndPrFromConfig(migrationCommitData)
      }
    }
  }
}
