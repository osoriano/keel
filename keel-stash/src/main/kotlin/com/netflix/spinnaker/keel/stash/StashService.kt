package com.netflix.spinnaker.keel.stash

import com.netflix.spinnaker.keel.api.stash.BranchResponse
import com.netflix.spinnaker.keel.api.stash.BuildResult
import com.netflix.spinnaker.keel.api.stash.Commit
import com.netflix.spinnaker.keel.api.stash.PullRequest
import com.netflix.spinnaker.keel.api.stash.Repo
import retrofit2.Response
import retrofit2.http.Body
import retrofit2.http.DELETE
import retrofit2.http.GET
import retrofit2.http.Multipart
import retrofit2.http.POST
import retrofit2.http.PUT
import retrofit2.http.Part
import retrofit2.http.Path

/**
 * Stash interface.
 * Documentation: https://docs.atlassian.com/bitbucket-server/rest/7.18.0/bitbucket-rest.html
 */
interface StashService {

  @POST("/rest/api/1.0/projects/{projectKey}/repos/{repositorySlug}/pull-requests")
  suspend fun openPullRequest(
    @Path("projectKey") projectKey: String,
    @Path("repositorySlug") repositorySlug: String,
    @Body pullRequest: PullRequest
  ): PullRequest

  @Multipart
  @PUT("/rest/api/1.0/projects/{projectKey}/repos/{repositorySlug}/browse/{path}")
  suspend fun addCommit(
    @Path("projectKey") projectKey: String,
    @Path("repositorySlug") repositorySlug: String,
    @Path(value = "path", encoded = true) path: String,
    @Part("branch") branch: String,
    @Part("sourceBranch", encoding = "8-bit") sourceBranch: String? = null,
    @Part("message") message: String,
    @Part("content") content: String,
    @Part("sourceCommitId") sourceCommitId: String? = null
  ): Commit


  /**
   * Retrieve a single commit identified by its ID
   */
  @GET("/rest/api/1.0/projects/{projectKey}/repos/{repositorySlug}/commits/{commitId}")
  suspend fun getCommit(
    @Path("projectKey") projectKey: String,
    @Path("repositorySlug") repositorySlug: String,
    @Path("commitId") commitId: String
  ): Commit

  @GET("/rest/api/1.0/projects/{projectKey}/repos/{repositorySlug}/branches/default")
  suspend fun getDefaultBranch(
    @Path("projectKey") projectKey: String,
    @Path("repositorySlug") repoSlug: String
  ): BranchResponse


  /**
   * Creates a fork of the source project and repository, into the project defined in [forkRequest]
   * The user making this request must have an admin permission into the [forkRequest] project.
   */
  @POST("/rest/api/1.0/projects/{projectKey}/repos/{repositorySlug}")
  suspend fun createFork(
    @Path("projectKey") projectKey: String,
    @Path("repositorySlug") repositorySlug: String,
    @Body forkRequest: Repo
  )

  /**
   * Delete a fork of the source repository in  repository
   */
  @DELETE("/rest/api/1.0/projects/{projectKey}/repos/{repositorySlug}")
  suspend fun deleteFork(
    @Path("projectKey") projectKey: String,
    @Path("repositorySlug") repositorySlug: String
  )

  /**
   * Store a build status for a commit [commitId], and a specific repo and project.
   */
  @POST("/rest/api/1.0/projects/{projectKey}/repos/{repositorySlug}/commits/{commitId}/builds")
  suspend fun storeBuildStatus(
    @Path("projectKey") projectKey: String,
    @Path("repositorySlug") repositorySlug: String,
    @Path("commitId") commitId: String,
    @Body buildResult: BuildResult
  ): Response<Unit>
}
