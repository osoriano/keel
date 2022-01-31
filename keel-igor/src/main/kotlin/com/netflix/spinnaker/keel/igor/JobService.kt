package com.netflix.spinnaker.keel.igor
import com.netflix.spinnaker.keel.igor.model.Job
import retrofit2.http.GET
import retrofit2.http.Headers
import retrofit2.http.Path
import retrofit2.http.Query

interface JobService {

  /**
   * Get CI jobs given project key and repo slug.
   *
   * @param projectKey the project key
   * @param repoSlug the repository name
   * @param type the scm service (e.g. Stash)
   * @param scmType the sub type of the scm service
   * @return a list of jobs
   */
  @GET("/ci/hasJobs")
  @Headers("Accept: application/json")
  suspend fun hasJobs(
    @Query("projectKey") projectKey: String,
    @Query("repoSlug") repoSlug: String,
    @Query("type") type: String? = null,
    @Query("scmType") scmType: List<String>? = null
  ): Boolean


  /**
   * Get a CI job by job name
   */
  @GET("/ci/jobs/{jobName}")
  @Headers("Accept: application/json")
  suspend fun getJobByName(
    @Path("jobName") jobName: String
  ): Job
}
