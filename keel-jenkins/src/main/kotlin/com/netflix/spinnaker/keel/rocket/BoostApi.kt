package com.netflix.spinnaker.keel.rocket

import com.netflix.spinnaker.keel.jenkins.JobState
import retrofit2.http.GET
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
}
