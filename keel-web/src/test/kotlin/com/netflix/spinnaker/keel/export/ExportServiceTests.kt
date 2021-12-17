package com.netflix.spinnaker.keel.export

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spinnaker.config.BaseUrlConfig
import com.netflix.spinnaker.keel.api.migration.SkipReason
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.model.Credential
import com.netflix.spinnaker.keel.core.api.SubmittedResource
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.model.Application
import com.netflix.spinnaker.keel.front50.model.Pipeline
import com.netflix.spinnaker.keel.igor.JobService
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.test.configuredTestObjectMapper
import com.netflix.spinnaker.keel.test.mockEnvironment
import com.netflix.spinnaker.keel.test.submittedDeliveryConfig
import com.netflix.spinnaker.keel.test.titusCluster
import com.netflix.spinnaker.keel.titus.TitusClusterHandler
import com.netflix.spinnaker.keel.validators.DeliveryConfigValidator
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isSuccess
import strikt.assertions.isTrue
import io.mockk.coEvery as every
import io.mockk.coVerify as verify

internal class ExportServiceTests {
  private val front50Cache: Front50Cache = mockk()
  private val orcaService: OrcaService = mockk()
  private val yamlMapper: YAMLMapper = mockk()
  private val deliveryConfigRepository: DeliveryConfigRepository = mockk()
  private val jobService: JobService = mockk()
  private val cloudDriverCache: CloudDriverCache = mockk()
  private val titusClusterHandler: TitusClusterHandler = mockk()
  private val objectMapper = configuredTestObjectMapper()

  private val subject = ExportService(
    handlers = listOf(titusClusterHandler),
    front50Cache = front50Cache,
    orcaService = orcaService,
    baseUrlConfig = BaseUrlConfig(),
    yamlMapper = yamlMapper,
    validator = DeliveryConfigValidator(),
    deliveryConfigRepository = deliveryConfigRepository,
    jobService = jobService,
    springEnv = mockEnvironment(),
    scmConfig = ScmConfig(),
    cloudDriverCache = cloudDriverCache
  )

  private val deliveryCofig = submittedDeliveryConfig()

  private val pipeline = Pipeline(
    name = "pipeline",
    id = "1",
    application = "fnord",
  )

  private val exportResult = PipelineExportResult(
    deliveryConfig = deliveryCofig,
    baseUrl = "https://baseurl.com",
    exported = mapOf(pipeline to deliveryCofig.environments.toList()),
    projectKey = "spkr",
    repoSlug = "keel",
    configValidationException = null,
    skipped = emptyMap()
  )

  private val titusCluster = titusCluster()

  private val testAccount =
    Credential("test", "titus", "test",
      mutableMapOf("regions" to titusCluster.spec.locations.regions.map {
        mapOf("name" to it.name)
      })
    )

  @BeforeEach
  fun setup() {
    every {
      front50Cache.applicationByName("fnord")
    } returns Application(name = "fnord", repoType = "stash", repoProjectKey = "spkr", repoSlug = "fnord")

    every {
      deliveryConfigRepository.updateMigratingAppScmStatus("fnord", any())
    } just runs

    every {
      titusClusterHandler.supportedKind
    } returns TITUS_CLUSTER_V1

    every {
      titusClusterHandler.export(any())
    } returns titusCluster.spec

    every {
      cloudDriverCache.credentialBy(any())
    } returns testAccount

    objectMapper.registerSubtypes(NamedType(TitusClusterSpec::class.java, TITUS_CLUSTER_V1.kind.toString()))
  }

  @Test
  fun `store application as scm-powered`() {
    every {
      jobService.hasJobs(any(), any(), any(), any())
    } returns true

    expectCatching {
      subject.updateApplicationScmStatus("fnord")
    }.isSuccess()

    verify(exactly = 1) {
      deliveryConfigRepository.updateMigratingAppScmStatus("fnord", true)
    }
  }

  @Test
  fun `store application as non scm-powered`() {
    every {
      jobService.hasJobs(any(), any(), any(), any())
    } returns false

    expectCatching {
      subject.updateApplicationScmStatus("fnord")
    }.isSuccess()

    verify(exactly = 1) {
      deliveryConfigRepository.updateMigratingAppScmStatus("fnord", false)
    }
  }

  @Test
  fun `export is successful`() {
    expectThat(exportResult.exportSucceeded).isTrue()
  }

  @Test
  fun `export is successful if only has old and disabled pipelines`() {
    listOf(SkipReason.DISABLED, SkipReason.NOT_EXECUTED_RECENTLY).forEach {
      expectThat(exportResult.copy(skipped = mapOf(pipeline to it)).exportSucceeded).isTrue()
    }
  }

  @Test
  fun `export is not successful if some pipelines failed to export`() {
    listOf(SkipReason.SHAPE_NOT_SUPPORTED, SkipReason.HAS_PARALLEL_STAGES, SkipReason.FROM_TEMPLATE).forEach {
      expectThat(exportResult.copy(skipped = mapOf(pipeline to it)).exportSucceeded).isFalse()
    }
  }

  @Test
  fun `exporting a resource delegates to the correct handler`() {
    val exported = runBlocking {
      subject.exportResource("titus", "cluster", titusCluster.spec.locations.account, titusCluster.name, "keel@keel.io")
    }
    verify { titusClusterHandler.export(any()) }
    expectThat(exported.spec).isEqualTo(titusCluster.spec)
  }

  @Test
  fun `re-exporting a resource delegates to the correct handler`() {
    val submittedResource = objectMapper.convertValue<SubmittedResource<TitusClusterSpec>>(titusCluster)
    val exported = runBlocking {
      subject.reExportResource(submittedResource, "keel@keel.io")
    }
    verify { titusClusterHandler.export(any()) }
    expectThat(exported.spec).isEqualTo(titusCluster.spec)
  }
}
