package com.netflix.spinnaker.keel.titus.registry

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spinnaker.config.DefaultWorkhorseCoroutineContext
import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.config.FeatureToggles.Companion.OPTIMIZED_DOCKER_FLOW
import com.netflix.spinnaker.config.RegistryCacheProperties
import com.netflix.spinnaker.config.WorkhorseCoroutineContext
import com.netflix.spinnaker.keel.api.artifacts.DockerImage
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.kork.exceptions.IntegrationException
import kotlinx.coroutines.CoroutineScope
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder.DESC
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Interface to the Titus Registry image data cache backed by ElasticSearch, with a fallback to the
 * CloudDriver image cache (which periodically syncs from ES).
 *
 * @link https://manuals.netflix.net/view/titus-docs/mkdocs/master/registry/#elasticsearch
 * @link https://www.elastic.co/guide/en/elasticsearch/client/java-rest/5.6/java-rest-high-search.html
 */
@Component
class TitusRegistryService(
  private val config: RegistryCacheProperties,
  private val elasticSearchClient: RestHighLevelClient,
  private val cloudDriverCache: CloudDriverCache,
  private val cloudDriverService: CloudDriverService,
  private val featureToggles: FeatureToggles,
  private val objectMapper: ObjectMapper,
  override val coroutineContext: WorkhorseCoroutineContext = DefaultWorkhorseCoroutineContext
) : CoroutineScope {
  companion object {
    private val log: Logger by lazy { LoggerFactory.getLogger(TitusRegistryService::class.java) }
    const val DEFAULT_MAX_RESULTS = 60 // (10 versions back * 3 regions * 2 registries)
  }

  /**
   * Searches the Titus Registry for images in the specified [image] and (optionally) [titusAccount],
   * with the specified [tag] (optional) and [digest] (optional), up to the maximum [limit] of results.
   * This method will first try the Titus Registry ElasticSearch mirror if the [OPTIMIZED_DOCKER_FLOW] feature
   * toggle is enabled, and fallback to calling [cloudDriverService] if the results from ElasticSearch are empty,
   * or if the toggle is disabled.
   */
  suspend fun findImages(
    image: String,
    titusAccount: String? = null,
    tag: String? = null,
    digest: String? = null,
    limit: Int = DEFAULT_MAX_RESULTS
  ): List<DockerImage> {
    return if (featureToggles.isEnabled(OPTIMIZED_DOCKER_FLOW)) {
      val awsAccount = titusAccount?.let { cloudDriverCache.getAwsAccountNameForTitusAccount(it) }
      findImagesInElasticSearch(image, awsAccount, tag, digest, limit)
    } else {
      emptyList()
    }.ifEmpty {
      // fallback to CloudDriver if the optimized flow is off or ElasticSearch returns no results
      val registry = titusAccount?.let { cloudDriverCache.getRegistryForTitusAccount(it) } ?: "*"
      log.debug("Searching CloudDriver image cache (repository: $image, tag: $tag, registry: $registry)")
      cloudDriverService.findDockerImages(
        registry = registry,
        repository = image,
        tag = tag,
        includeDetails = true
      )
    }
  }

  /**
   * Searches the Titus Registry ElasticSearch index for images with the specified [repository],
   * [awsAccount] (optional), [tag] (optional) and [digest] (optional), up to the maximum [limit] of results.
   */
  fun findImagesInElasticSearch(
    repository: String,
    awsAccount: String? = null,
    tag: String? = null,
    digest: String? = null,
    limit: Int = DEFAULT_MAX_RESULTS
  ): List<DockerImage> {
    val imageCoordinates = DockerImageCoordinates(repository, awsAccount, tag, digest)
    log.debug("Searching ElasticSearch Titus registry cache for $imageCoordinates")
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
              // we don't care about the "latest" tag
              ?: mustNot(QueryBuilders.termQuery("tag.keyword", "latest"))
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
        .sort("date", DESC)

      val searchRequest = SearchRequest(config.index).source(sourceBuilder)
      val searchResponse = elasticSearchClient.search(searchRequest)
      log.debug("Got response from ElasticSearch: returning ${searchResponse.hits.hits.size}/" +
        "${searchResponse.hits.totalHits} hits for $imageCoordinates")

      val images = searchResponse.hits.hits.mapNotNull {
        val parsedImage = it.sourceAsMap.let { imageAsMap ->
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
        if (parsedImage == null) {
          log.debug("Newt labels missing for $imageCoordinates. Ignoring.")
        }
        parsedImage
      }
      val filteredImages = images.groupBy { "${it.account}/${it.region.orEmpty()}" }.values.firstOrNull() ?: emptyList()
      log.debug("Parsed ${images.size} total images for $imageCoordinates. Returning ${filteredImages.size} from first account and region.")
      return filteredImages
    } catch (e: Exception) {
      log.debug("Error searching for Docker image at $imageCoordinates: $e", e)
      throw IntegrationException("Unable to retrieve information for Docker image $repository:${tag ?: "*"} from Titus registry cache.", e)
    }
  }

  private class DockerImageCoordinates(
    val repository: String,
    val awsAccount: String?,
    val tag: String?,
    val digest: String?
  ) {
    override fun toString() = "$repository:${tag ?: "*"} (account: ${awsAccount ?: "*"}, digest: ${digest ?: "*"})"
  }
}
