package com.netflix.spinnaker.keel.titus.registry

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.config.FeatureToggles.Companion.OPTIMIZED_DOCKER_FLOW
import com.netflix.spinnaker.config.RegistryCacheProperties
import com.netflix.spinnaker.keel.api.artifacts.DockerImage
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import io.mockk.called
import io.mockk.coEvery as every
import io.mockk.coVerify as verify
import io.mockk.mockk
import io.mockk.slot
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.sort.SortBuilders
import org.elasticsearch.search.sort.SortOrder.DESC
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import java.time.Instant
import java.util.stream.Stream
import org.junit.jupiter.params.provider.Arguments.of
import strikt.assertions.contains
import strikt.assertions.first
import strikt.assertions.hasSize


internal class TitusRegistryServiceTests {
  private val cloudDriverCache: CloudDriverCache = mockk()
  private val cloudDriverService: CloudDriverService = mockk()
  private val featureToggles: FeatureToggles = mockk()
  private val objectMapper = configuredObjectMapper()
  private val config = RegistryCacheProperties("host", "index")
  private val elasticSearchClient: RestHighLevelClient = mockk()
  private val searchResponse: SearchResponse = mockk()
  private val searchHits: SearchHits = mockk()
  private val imageHit1: SearchHit = mockk()
  private val imageHit2: SearchHit = mockk()
  private val subject = TitusRegistryService(config, elasticSearchClient, cloudDriverCache, cloudDriverService, featureToggles, objectMapper)

  @BeforeEach
  fun setup() {
    every {
      featureToggles.isEnabled(OPTIMIZED_DOCKER_FLOW)
    } returns true

    every {
      cloudDriverService.findDockerImages(any(), any(), any(), any(), any())
    } returns emptyList()

    every {
      cloudDriverCache.getRegistryForTitusAccount(any())
    } returns registry

    every {
      cloudDriverCache.getAwsAccountNameForTitusAccount(any())
    } returns awsAccount

    every {
      elasticSearchClient.search(any())
    } returns searchResponse

    every {
      searchResponse.hits
    } returns searchHits

    every {
      searchHits.hits
    } returns arrayOf(imageHit1, imageHit2)

    every {
      imageHit1.sourceAsMap
    } returns imageAsMap1

    every {
      imageHit2.sourceAsMap
    } returns imageAsMap2
  }

  @Test
  fun `uses ElasticSearch when optimized Docker flow is enabled`() {
    subject.findImages(image, titusAccount, tag, digest)

    verify {
      elasticSearchClient.search(any())
    }

    verify {
      cloudDriverService wasNot called
    }
  }

  @Test
  fun `falls back to CloudDriver if ElasticSearch returns empty results`() {
    every {
      searchHits.hits
    } returns emptyArray()

    subject.findImages(image, titusAccount, tag, digest)

    verify {
      elasticSearchClient.search(any())
    }

    verify {
      cloudDriverService.findDockerImages(registry, image, tag, includeDetails = true)
    }
  }

  @Test
  fun `falls back to CloudDriver when optimized Docker flow is disabled`() {
    every {
      featureToggles.isEnabled(OPTIMIZED_DOCKER_FLOW)
    } returns false

    subject.findImages(image, titusAccount, tag, digest)

    verify {
      cloudDriverService.findDockerImages(registry, image, tag, includeDetails = true)
    }

    verify {
      elasticSearchClient wasNot called
    }
  }

  @Test
  fun `correctly parses ElasticSearch results`() {
    val images = subject.findImages(image, titusAccount, tag, digest)
    expectThat(images)
      .first().isEqualTo(
        DockerImage(
          account = titusAccount,
          repository = image,
          tag = tag,
          digest = digest,
          buildNumber = buildNumber,
          commitId = commitId,
          prCommitId = commitId,
          branch = branch,
          date = date,
          region = region
        )
      )
  }

  @Test
  fun `dedupes images from ElasticSearch results`() {
    val images = subject.findImages(image, titusAccount, tag, digest)
    expectThat(searchHits.hits.size).isEqualTo(2)
    expectThat(images).hasSize(1)
  }

  @ParameterizedTest(name = "titusAccount: {0}, tag: {1}, digest: {2}")
  @MethodSource("testParameters")
  fun `includes non-null parameters in the ElasticSearch query`(titusAccount: String?, tag: String?, digest: String?) {
    subject.findImages(image, titusAccount, tag, digest)

    val searchRequest = slot<SearchRequest>()
    verify {
      elasticSearchClient.search(capture(searchRequest))
    }

    expectThat(searchRequest.captured) {
      get { indices().first() }.isEqualTo(config.index)
      get { source().query() }.isA<BoolQueryBuilder>().and {
        get { must() }.and {
          contains(TermQueryBuilder("repository.keyword", image))
          titusAccount?.also { contains(TermQueryBuilder("account", awsAccount)) }
          tag?.also { contains(TermQueryBuilder("tag.keyword", it)) }
          digest?.also { contains(TermQueryBuilder("digest", it)) }
        }
        tag ?: get { mustNot() }.contains(TermQueryBuilder("tag.keyword", "latest"))
      }
    }
  }

  @Test
  fun `sorts ElasticSearch results by descending order of date`() {
    subject.findImages(image, titusAccount, tag, digest)

    val searchRequest = slot<SearchRequest>()
    verify {
      elasticSearchClient.search(capture(searchRequest))
    }

    expectThat(searchRequest.captured) {
      get { source().sorts() }
        .hasSize(1)
        .first().isEqualTo(SortBuilders.fieldSort("date").order(DESC))
    }
  }

  companion object {
    private const val titusAccount = "titustest"
    private const val awsAccount = "awstest"
    private const val registry = "testregistry"
    private const val region = "us-east-1"
    private const val image = "lpollo/lpollo-md-prestaging"
    private const val tag = "master-h2.44021ac"
    private const val digest = "sha256:41af286dc0b172ed2f1ca934fd2278de4a1192302ffa07087cea2682e7d372e3"
    private const val buildNumber = "1"
    private const val commitId = "259fc39c0e19a448305c57a17d939115ec28aafe"
    private const val branch = "main"
    private val date = Instant.now().toEpochMilli().toString()
    private val imageAsMap1 = mapOf(
      "account" to titusAccount,
      "repository" to image,
      "tag" to tag,
      "digest" to digest,
      "date" to date,
      "region" to region,
      "newt_labels" to mapOf(
        "jenkins-build" to buildNumber,
        "git-commit" to commitId,
        "git-pr-commit" to commitId,
        "git-branch" to branch
      )
    )
    private val imageAsMap2 = imageAsMap1.toMutableMap().apply { put("region", "us-west-2") }

    @JvmStatic
    private fun testParameters(): Stream<Arguments> = Stream.of(
      of(null, null, null),
      of(titusAccount, null, null),
      of(titusAccount, tag, null),
      of(titusAccount, tag, digest)
    )
  }
}
