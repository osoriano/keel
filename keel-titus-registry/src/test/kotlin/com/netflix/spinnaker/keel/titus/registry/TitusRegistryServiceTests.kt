package com.netflix.spinnaker.keel.titus.registry

import com.netflix.spinnaker.config.RegistryCacheProperties
import com.netflix.spinnaker.keel.api.artifacts.DockerImage
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.SearchHits
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
  private val objectMapper = configuredObjectMapper()
  private val config = RegistryCacheProperties("host", "index")
  private val elasticSearchClient: RestHighLevelClient = mockk()
  private val searchResponse: SearchResponse = mockk()
  private val searchHits: SearchHits = mockk()
  private val imageHit: SearchHit = mockk()
  private val subject = TitusRegistryService(config, elasticSearchClient, objectMapper)

  @BeforeEach
  fun setup() {
    every {
      elasticSearchClient.search(any())
    } returns searchResponse

    every {
      searchResponse.hits
    } returns searchHits

    every {
      searchHits.hits
    } returns arrayOf(imageHit)

    every {
      imageHit.sourceAsMap
    } returns imageAsMap
  }

  @Test
  fun `correctly parses search results`() {
    val images = subject.findImages(image, account, tag, digest)
    expectThat(images)
      .hasSize(1)
      .first().isEqualTo(
        DockerImage(
          account = account,
          repository = image,
          tag = tag,
          digest = digest,
          buildNumber = buildNumber,
          commitId = commitId,
          prCommitId = commitId,
          branch = branch,
          date = date
        )
      )
  }

  @ParameterizedTest(name = "account: {0}, tag: {1}, digest: {2}")
  @MethodSource("testParameters")
  fun `includes non-null parameters in the search`(account: String?, tag: String?, digest: String?) {
    subject.findImages(image, account, tag, digest)

    val searchRequest = slot<SearchRequest>()
    verify {
      elasticSearchClient.search(capture(searchRequest))
    }

    expectThat(searchRequest.captured) {
      get { indices().first() }.isEqualTo(config.index)
      get { source().query() }.isA<BoolQueryBuilder>().and {
        get { must() }.and {
          contains(TermQueryBuilder("repository.keyword", image))
          account?.also {
            contains(TermQueryBuilder("account", it))
          }
          tag?.also {
            contains(TermQueryBuilder("tag.keyword", it))
          }
          digest?.also {
            contains(TermQueryBuilder("digest", it))
          }
        }
      }
    }
  }

  companion object {
    private const val account = "test"
    private const val image = "lpollo/lpollo-md-prestaging"
    private const val tag = "master-h2.44021ac"
    private const val digest = "sha256:41af286dc0b172ed2f1ca934fd2278de4a1192302ffa07087cea2682e7d372e3"
    private const val buildNumber = "1"
    private const val commitId = "259fc39c0e19a448305c57a17d939115ec28aafe"
    private const val branch = "main"
    private val date = Instant.now().toEpochMilli().toString()
    private val imageAsMap = mapOf(
      "account" to account,
      "repository" to image,
      "tag" to tag,
      "digest" to digest,
      "date" to date,
      "newt_labels" to mapOf(
        "jenkins-build" to buildNumber,
        "git-commit" to commitId,
        "git-pr-commit" to commitId,
        "git-branch" to branch
      )
    )

    @JvmStatic
    private fun testParameters(): Stream<Arguments> = Stream.of(
      of(null, null, null),
      of(account, null, null),
      of(account, tag, null),
      of(account, tag, digest)
    )
  }
}
