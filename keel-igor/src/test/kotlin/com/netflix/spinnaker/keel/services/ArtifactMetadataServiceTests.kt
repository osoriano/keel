package com.netflix.spinnaker.keel.services

import com.netflix.spinnaker.keel.api.artifacts.shortHash
import com.netflix.spinnaker.keel.igor.BuildService
import com.netflix.spinnaker.keel.igor.artifact.ArtifactMetadataService
import com.netflix.spinnaker.keel.igor.artifact.toArtifactMetadata
import com.netflix.spinnaker.keel.igor.model.Build
import com.netflix.spinnaker.keel.igor.model.GenericGitRevision
import com.netflix.spinnaker.keel.igor.model.Result.SUCCESS
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import retrofit.RetrofitError
import retrofit.client.Response
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isNull

class ArtifactMetadataServiceTests : JUnit5Minutests {
  object Fixture {
    val buildService: BuildService = mockk()
    val artifactMetadataService = ArtifactMetadataService(buildService)
    val longHash = "970318968feb640da723b8826861e41f0718a487"
    val shortHash = longHash.shortHash
    val matchingBuild = Build(
      number = 1,
      name = "job bla bla",
      id = "1234",
      building = false,
      fullDisplayName = "job bla bla",
      url = "jenkins.com",
      result = SUCCESS,
      scm = listOf(
        GenericGitRevision(
          sha1 = longHash,
          message = "this is a commit message",
          committer = "keel-user",
          compareUrl = "https://github.com/spinnaker/keel/commit/a15p0"
        )
      ),
      properties = mapOf(
        "startedAt" to "yesterday",
        "completedAt" to "today",
        "projectKey" to "spkr",
        "repoSlug" to "keel",
        "author" to "keel-user",
        "message" to "this is a commit message",
        "pullRequestNumber" to "111",
        "pullRequestUrl" to "www.github.com/pr/111"
      )
    )

    val buildsList: List<Build> = listOf(matchingBuild)

    val expectedMetadata = matchingBuild.toArtifactMetadata()
  }

  fun tests() = rootContext<Fixture> {
    context("get artifact metadata") {
      fixture { Fixture }

      context("with valid commit id and build number") {
        before {
          coEvery {
            buildService.getArtifactMetadata(commitId = any(), buildNumber = any(), completionStatus = any())
          } returns buildsList
        }

        test("succeeds and converts the results correctly") {
          val result = runBlocking {
            artifactMetadataService.getArtifactMetadata("1", shortHash)
          }
          expectThat(result).isEqualTo(expectedMetadata)
        }

        test("commit id length is long, expect short commit in return") {
          val result = runBlocking {
            artifactMetadataService.getArtifactMetadata("1", longHash)
          }
          expectThat(result).get {
            result?.gitMetadata?.commit
          }.isEqualTo(shortHash)
        }
      }

      context("with empty results from the CI provider") {
        before {
          coEvery {
            buildService.getArtifactMetadata(commitId = any(), buildNumber = any(), completionStatus = any())
          } returns listOf()
        }

        test("returns null") {
          val results = runBlocking {
            artifactMetadataService.getArtifactMetadata("1", shortHash, maxAttempts = 1)
          }
          expectThat(results).isNull()
        }
      }

      context("with HTTP error coming from igor") {
        val retrofitError = RetrofitError.httpError(
          "http://igor",
          Response("http://igor", 404, "not found", emptyList(), null),
          null, null
        )

        before {
          coEvery {
            buildService.getArtifactMetadata(commitId = any(), buildNumber = any(), completionStatus = any())
          } throws retrofitError
        }

        test("throw an http error from fallback method") {
          expectCatching {
            artifactMetadataService.getArtifactMetadata("1", shortHash)
          }
            .isFailure()
            .isEqualTo(retrofitError)
        }
      }

      context("with non-matching builds in the response") {
        context("when at least one build matches keel's metadata") {
          before {
            coEvery {
              buildService.getArtifactMetadata(commitId = any(), buildNumber = any(), completionStatus = any())
            } returns listOf(
              matchingBuild.copy(scm = listOf(matchingBuild.scm.first().copy(sha1 = "nomatch"))),
              matchingBuild.copy(number = -1),
              matchingBuild
            )
          }

          test("ignores non-matching builds and returns the metadata based on the first applicable build") {
            val results = runBlocking {
              artifactMetadataService.getArtifactMetadata("1", shortHash, maxAttempts = 1)
            }
            expectThat(results).isEqualTo(expectedMetadata)
          }
        }

        context("when no builds match keel's metadata") {
          before {
            coEvery {
              buildService.getArtifactMetadata(commitId = any(), buildNumber = any(), completionStatus = any())
            } returns listOf(
              matchingBuild.copy(scm = listOf(matchingBuild.scm.first().copy(sha1 = "nomatch"))),
              matchingBuild.copy(number = -1)
            )
          }

          test("returns null") {
            val results = runBlocking {
              artifactMetadataService.getArtifactMetadata("1", shortHash, maxAttempts = 1)
            }
            expectThat(results).isNull()
          }
        }
      }
    }
  }
}
