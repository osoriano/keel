package com.netflix.spinnaker.keel.rocket

import com.netflix.spinnaker.keel.igor.model.Build
import com.netflix.spinnaker.keel.jenkins.JobState
import retrofit2.http.GET
import retrofit2.http.Headers
import retrofit2.http.Query

/**
 * REST interface to the Rocket Boost API (wrapper for Jenkins API).
 */
interface BoostApi {
  @GET("/api/v1/jobs/search-by-name")
  suspend fun findJob(
    @Query("master") controller: String? = null,
    @Query("name") job: String,
    @Query("state") state: JobState? = null
  ): JobsResponse


  @GET("/api/v1/builds")
  @Headers("Accept: application/json")
  suspend fun getArtifactMetadata(
    @Query("commitId") commitId: String,
    @Query("buildNumber") buildNumber: String,
    @Query("projectKey") projectKey: String? = null,
    @Query("repoSlug") repoSlug: String? = null,
    @Query("completionStatus") completionStatus: String? = null
  ): List<Build>?
}
