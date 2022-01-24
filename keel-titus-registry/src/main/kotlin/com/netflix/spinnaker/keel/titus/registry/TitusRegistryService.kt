package com.netflix.spinnaker.keel.titus.registry

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spinnaker.config.RegistryCacheProperties
import com.netflix.spinnaker.keel.api.artifacts.DockerImage
import com.netflix.spinnaker.kork.exceptions.IntegrationException
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Interface to the Titus Registry image data cache backed by ElasticSearch.
 *
 * @link https://manuals.netflix.net/view/titus-docs/mkdocs/master/registry/#elasticsearch
 * @link https://www.elastic.co/guide/en/elasticsearch/client/java-rest/5.6/java-rest-high-search.html
 */
@Component
class TitusRegistryService(
  private val config: RegistryCacheProperties,
  private val elasticSearchClient: RestHighLevelClient,
  private val objectMapper: ObjectMapper
) {
  companion object {
    private val log: Logger by lazy { LoggerFactory.getLogger(TitusRegistryService::class.java) }
    const val DEFAULT_MAX_RESULTS = 50
  }

  /**
   * Searches the Titus Registry ElasticSearch index for images with the specified [repository],
   * [awsAccount] (optional), [tag] (optional) and [digest] (optional), up to the maximum [limit] of results.
   */
  fun findImages(
    repository: String,
    awsAccount: String? = null,
    tag: String? = null,
    digest: String? = null,
    limit: Int = DEFAULT_MAX_RESULTS
  ): List<DockerImage> {
    log.debug("Searching ElasticSearch Titus registry cache (repository: $repository, tag: $tag, account: $awsAccount, digest: $digest")
    try {
      val sourceBuilder = SearchSourceBuilder()
      sourceBuilder
        .query(
          QueryBuilders.boolQuery().apply {
            // repository (the image name) is mandatory
            must(QueryBuilders.termQuery("repository.keyword", repository))
            // account is optional (if not specify, we search across all accounts)
            awsAccount?.also { must(QueryBuilders.termQuery("account", awsAccount)) }
            // tag is optional (if not specified, and digest is also not specified, we return all tags)
            tag?.also { must(QueryBuilders.termQuery("tag.keyword", it)) }
            // digest is optional (if specified, we return only images with a matching digest)
            digest?.also { must(QueryBuilders.termQuery("digest", it)) }
          }
        )
        .fetchSource(
          // included fields
          arrayOf("account", "repository", "tag", "region", "date", "digest", "newt_labels.*"),
          // excluded fields
          emptyArray<String>()
        )
        .size(limit)

      val searchRequest = SearchRequest(config.index).source(sourceBuilder)
      val searchResponse = elasticSearchClient.search(searchRequest)
      log.debug("Got response from ElasticSearch: returning ${searchResponse.hits.hits.size}/${searchResponse.hits.totalHits} hits" +
        " for repository: $repository, tag: $tag, account: $awsAccount, digest: $digest")

      val images = searchResponse.hits.hits.mapNotNull {
        it.sourceAsMap.let { imageAsMap ->
          (imageAsMap["newt_labels"] as? Map<String, String>)
            ?.let { labels ->
              objectMapper.convertValue<DockerImage>(imageAsMap).copy(
                buildNumber = labels["jenkins-build"],
                commitId = labels["git-commit"],
                prCommitId = labels["git-pr-commit"],
                branch = labels["git-branch"]
              )
            }
        }
      }
      return images
    } catch (e: Exception) {
      log.debug("Error retrieving details for Docker image $repository:$tag from account $awsAccount: $e", e)
      throw IntegrationException("Unable to retrieve details for Docker image $repository:$tag from Titus registry cache.", e)
    }
  }
}
