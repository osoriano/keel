package com.netflix.spinnaker.keel.igor
import com.netflix.spinnaker.keel.igor.model.ArtifactContents
import com.netflix.spinnaker.keel.igor.model.Build
import retrofit2.http.GET
import retrofit2.http.Headers
import retrofit2.http.Path
import retrofit2.http.Query

interface BuildService {

  /**
   * Retrieves build information by commit id and build number from a CI service.
   *
   * @param projectKey The "project" within the SCM system where the repository exists, which can be a user's personal
   *        area (e.g. "SPKR", "~<username>")
   * @param repoSlug The repository name (e.g. "myapp")
   * @param commitId The commit id.
   * @param buildNumber the build number .
   */
  @GET("/ci/builds/all")
  @Headers("Accept: application/json")
  suspend fun getArtifactMetadata(
    @Query("commitId") commitId: String,
    @Query("buildNumber") buildNumber: String,
    @Query("projectKey") projectKey: String? = null,
    @Query("repoSlug") repoSlug: String? = null,
    @Query("completionStatus") completionStatus: String? = null
  ): List<Build>?

  /**
   * Retrieves the contents of the specified artifact published from a build.
   */
  @GET("/builds/artifactContents/{master}/{job}/{buildNumber}")
  @Headers("Accept: application/json")
  suspend fun getArtifactContents(
    @Path("master") controller: String,
    @Path("job") job: String,
    @Path("buildNumber") buildNumber: Int,
    @Query("filePath") filePath: String
  ): ArtifactContents
}
