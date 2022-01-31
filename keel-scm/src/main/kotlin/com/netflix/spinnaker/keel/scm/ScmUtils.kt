package com.netflix.spinnaker.keel.scm

import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.caffeine.CacheFactory
import com.netflix.spinnaker.keel.core.ResourceCurrentlyUnresolvable
import com.netflix.spinnaker.keel.front50.model.Application
import com.netflix.spinnaker.keel.igor.ScmService
import com.netflix.spinnaker.keel.igor.getDefaultBranch
import com.netflix.spinnaker.keel.retrofit.isNotFound
import com.netflix.spinnaker.kork.exceptions.SystemException
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.springframework.stereotype.Component

@Component
class ScmUtils(
  cacheFactory: CacheFactory,
  private val scmService: ScmService,
) {
  private val cache = cacheFactory.asyncBulkLoadingCache("scmInfo") {
    scmService.getScmInfo()
  }

  fun getBranchLink(repoType: String?, repoProjectKey: String?, repoSlug: String?, branch: String?): String? {
    if (repoType == null || repoProjectKey == null || repoSlug == null || branch == null) {
      return null
    }

    val scmBaseUrl = getScmBaseLink(repoType) ?: return null

    return when (repoType) {
      "stash" -> "$scmBaseUrl/projects/$repoProjectKey/repos/$repoSlug/browse?at=refs/heads/$branch"
      "github" -> "$scmBaseUrl/$repoProjectKey/$repoSlug/tree/$branch"
      else -> null
    }
  }

  fun getCommitLink(event: CodeEvent): String? {
    return getCommitLink(event.repoType, event.projectKey, event.repoSlug, event.commitHash)
  }

  fun getCommitLink(repoType: String?, repoProjectKey: String?, repoSlug: String?, commitHash: String?): String? {
    if (repoType == null || repoProjectKey == null || repoSlug == null || commitHash == null) {
      return null
    }
    val scmBaseUrl = getScmBaseLink(repoType) ?: return null

    return when (repoType) {
      "stash" -> "$scmBaseUrl/projects/$repoProjectKey/repos/$repoSlug/commits/$commitHash"
      "github" -> "$scmBaseUrl/$repoProjectKey/$repoSlug/commit/$commitHash"
      else -> null
    }
  }

  fun getPullRequestLink(event: PrEvent): String? {
    return getPullRequestLink(event.repoType, event.projectKey, event.repoSlug, event.pullRequestId)
  }

  fun getPullRequestLink(repoType: String?, repoProjectKey: String?, repoSlug: String?, pullRequestId: String?): String? {
    if (repoType == null || repoProjectKey == null || repoSlug == null || pullRequestId == null) {
      return null
    }
    val scmBaseUrl = getScmBaseLink(repoType) ?: return null
    return when (repoType) {
      "stash" -> "$scmBaseUrl/projects/$repoProjectKey/repos/$repoSlug/pull-requests/$pullRequestId"
      "github" -> "$scmBaseUrl/$repoProjectKey/$repoSlug/pull/$pullRequestId"
      else -> null
    }
  }

  fun getScmBaseLink(repoType: String): String? = runBlocking {
    cache.get(repoType).await()
  }

  fun getDefaultBranch(application: Application): String {
    return application.getDefaultBranch(scmService)
  }

  /**
   * Fetch a GraphQL schema
   *
   * Throws NoSchemaFile on a 404 response
   */
  suspend fun fetchSchema(application: Application, gitMetadata: GitMetadata, schemaPath: String) : String {
    val repoType =
      application.repoType ?: error("Repository type missing from application config for ${application.name}")
    val projectKey = gitMetadata.project ?: error("Project name missing in git metadata")
    val repoSlug = gitMetadata.repo?.name ?: error("Repository name missing in git metadata")
    val ref = gitMetadata.commitInfo?.sha ?: error("Long commit hash missing in git metadata")
    try {
      return scmService.getGraphqlSchema(repoType, projectKey, repoSlug, ref, schemaPath).schema
    } catch(e: Exception) {
      when(e.isNotFound) {
        true -> throw NoSchemaFile(repoType, projectKey, repoSlug, ref, schemaPath)
        else -> throw e
      }
    }
  }
}

class NoSchemaFile(repoType: String, projectKey: String, repoSlug: String, ref: String, schemaPath: String) :
  SystemException(
      "GraphQL schema file(s) not found in path $schemaPath of repository $repoType/$projectKey/$repoSlug (commit $ref)"
  )

