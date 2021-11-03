package com.netflix.spinnaker.keel.igor.artifact

import com.netflix.spinnaker.keel.igor.BuildService
import com.netflix.spinnaker.keel.api.artifacts.ArtifactMetadata
import com.netflix.spinnaker.keel.api.artifacts.BuildMetadata
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.Job
import com.netflix.spinnaker.keel.api.artifacts.PullRequest
import com.netflix.spinnaker.keel.api.artifacts.Repo
import com.netflix.spinnaker.keel.igor.model.Build
import com.netflix.spinnaker.keel.igor.model.CompletionStatus
import io.github.resilience4j.kotlin.retry.executeSuspendFunction
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import kotlinx.coroutines.TimeoutCancellationException
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import retrofit2.HttpException
import java.time.Duration

/**
 * Provides functionality to convert build metadata, which is coming from internal service, to artifact metadata (via igor).
 */
@Component
class ArtifactMetadataService(
  private val buildService: BuildService
) {
  companion object {
    private const val DEFAULT_MAX_ATTEMPTS = 20
  }

  /**
   * Returns additional metadata about the specified build and commit, if available. This call is configured
   * to auto-retry as it's not on a code path where any external retries would happen.
   */
  suspend fun getArtifactMetadata(
    buildNumber: String,
    commitHash: String,
    maxAttempts: Int = DEFAULT_MAX_ATTEMPTS
  ): ArtifactMetadata? {
    if (commitHash.length < 7) {
      error("Invalid commit hash: $commitHash (min length is 7 characters, got ${commitHash.length})")
    }

    val artifactMetadata = getArtifactMetadataWithRetries(buildNumber, commitHash, maxAttempts)
      ?.firstOrNull {
        // filter out any builds that don't match the build number and commit hash known to Keel
        it.number.toString() == buildNumber && it.scm.firstOrNull()?.sha1?.shortHash == commitHash.shortHash
      }
      ?.toArtifactMetadata()

    if (artifactMetadata == null) {
      log.debug("artifact metadata is null for build $buildNumber and commit $commitHash")
      return null
    }

    log.debug("received artifact metadata for build $buildNumber and commit $commitHash: $artifactMetadata")
    return artifactMetadata
  }

  /**
   * Attempts to retrieve the [ArtifactMetadata] for the specified [commitHash] and [buildNumber] from the 
   * [buildService]. This function will retry up to [maxAttempts] before returning a null result.
   */
  private suspend fun getArtifactMetadataWithRetries(
    buildNumber: String,
    commitHash: String,
    maxAttempts: Int = DEFAULT_MAX_ATTEMPTS
  ): List<Build>? {
    val retry = Retry.of(
      "get artifact metadata",
      RetryConfig.custom<List<Build>?>()
        // retry 20 times over a total of 90 seconds
        .maxAttempts(maxAttempts)
        .waitDuration(Duration.ofMillis(4500))
        .retryOnResult{ result ->
          if (result.isNullOrEmpty()) {
            log.debug("Retrying artifact metadata retrieval due to empty response (commit=$commitHash, build=$buildNumber)")
          }
          result.isNullOrEmpty()
        }
        .retryOnException { t: Throwable ->
          // https://github.com/resilience4j/resilience4j/issues/688
          val retryFilter = when (t) {
            is TimeoutCancellationException -> true
            else -> t is HttpException
          }
          log.debug(if (retryFilter) "Retrying " else "Not retrying " +
            "artifact metadata retrieval (commit=$commitHash, build=$buildNumber) on exception: ${t::class.java.name}")
          retryFilter
        }
        .build()
    )

    return retry.executeSuspendFunction {
      buildService.getArtifactMetadata(
        commitId = commitHash.trim(),
        buildNumber = buildNumber.trim(),
        completionStatus = CompletionStatus.values().joinToString(",") { it.name }
      )
    }
  }

  private val String.shortHash: String
    get() = substring(0, 7)

  private val log by lazy { LoggerFactory.getLogger(javaClass) }
}

internal fun Build.toArtifactMetadata() =
  run {
    ArtifactMetadata(
      BuildMetadata(
        id = number,
        uid = id,
        job = Job(
          link = url,
          name = name
        ),
        startedAt = properties["startedAt"] as String?,
        completedAt = properties["completedAt"] as String?,
        number = number.toString(),
        status = result?.toString()
      ),
      GitMetadata(
        commit = scm.first().sha1.let {
          when {
            it == null -> error("Cannot parse git metadata from Build object with missing commit hash")
            it.length > 7 -> it.substring(0, 7)
            else -> it
          }
        },
        commitInfo = Commit(
          sha = scm.first().sha1,
          link = scm.first().compareUrl,
          message = scm.first().message,
        ),
        author = scm.first().committer,
        pullRequest = PullRequest(
          number = properties["pullRequestNumber"] as String?,
          url = properties["pullRequestUrl"] as String?
        ),
        repo = Repo(
          name = properties["repoSlug"] as String?,
          //TODO[gyardeni]: add link (will come from Igor)
          link = ""
        ),
        branch = scm.first().branch,
        project = properties["projectKey"] as String?,
      )
    )
  }
