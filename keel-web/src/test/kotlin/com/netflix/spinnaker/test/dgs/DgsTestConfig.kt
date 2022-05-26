package com.netflix.spinnaker.test.dgs

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.spinnaker.keel.actuation.ExecutionSummaryService
import com.netflix.spinnaker.keel.api.action.ActionRepository
import com.netflix.spinnaker.keel.artifacts.ArtifactVersionLinks
import com.netflix.spinnaker.keel.auth.AuthorizationSupport
import com.netflix.spinnaker.keel.bakery.BakeryMetadataService
import com.netflix.spinnaker.keel.buoy.BuoyClient
import com.netflix.spinnaker.keel.clouddriver.CloudDriverService
import com.netflix.spinnaker.keel.constraints.ConstraintEvaluators
import com.netflix.spinnaker.keel.dgs.ApplicationFetcherSupport
import com.netflix.spinnaker.keel.export.ExportService
import com.netflix.spinnaker.keel.extensions.DefaultExtensionRegistry
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.Front50Service
import com.netflix.spinnaker.keel.igor.DeliveryConfigImporter
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.persistence.ApplicationRepository
import com.netflix.spinnaker.keel.persistence.ArtifactRepository
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.persistence.DiffFingerprintRepository
import com.netflix.spinnaker.keel.persistence.DismissibleNotificationRepository
import com.netflix.spinnaker.keel.persistence.EnvironmentDeletionRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.LifecycleEventRepository
import com.netflix.spinnaker.keel.persistence.TaskTrackingRepository
import com.netflix.spinnaker.keel.schema.Generator
import com.netflix.spinnaker.keel.scm.ScmUtils
import com.netflix.spinnaker.keel.services.ApplicationService
import com.netflix.spinnaker.keel.services.ResourceStatusService
import com.netflix.spinnaker.keel.upsert.DeliveryConfigUpserter
import com.netflix.spinnaker.keel.veto.unhappy.UnhappyVeto
import com.netflix.springboot.scheduling.DefaultExecutor
import com.ninjasquad.springmockk.MockkBean
import io.mockk.mockk
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import java.util.concurrent.Executor
import java.util.concurrent.Executors

@Configuration
@ComponentScan(basePackages = ["com.netflix.spinnaker.keel.dgs"])
internal class DgsTestConfig {

  val cloudDriverService: CloudDriverService = mockk(relaxUnitFun = true)
  val bakeryMetadataService: BakeryMetadataService = mockk(relaxUnitFun = true)
  val extensionRegistry = DefaultExtensionRegistry(emptyList())

  @Bean
  fun applicationFetcherSupport() = ApplicationFetcherSupport(cloudDriverService, bakeryMetadataService)

  @Bean
  @DefaultExecutor
  fun executor(): Executor = Executors.newSingleThreadExecutor()

  @Bean
  fun constraintEvaluators() = ConstraintEvaluators(emptyList())

  @Bean
  fun generator() = Generator(extensionRegistry)

  @MockkBean
  lateinit var actuationPauser: ActuationPauser

  @MockkBean
  lateinit var artifactVersionLinks: ArtifactVersionLinks

  @MockkBean
  lateinit var notificationRepository: DismissibleNotificationRepository

  @MockkBean
  lateinit var scmUtils: ScmUtils

  @MockkBean
  lateinit var executionSummaryService: ExecutionSummaryService

  @MockkBean
  lateinit var yamlMapper: YAMLMapper

  @MockkBean
  lateinit var deliveryConfigImporter: DeliveryConfigImporter

  @MockkBean
  lateinit var environmentDeletionRepository: EnvironmentDeletionRepository

  @MockkBean
  lateinit var exportService: ExportService

  @MockkBean
  lateinit var front50Cache: Front50Cache

  @MockkBean
  lateinit var deliveryConfigUpserter: DeliveryConfigUpserter

  @MockkBean
  lateinit var lifecycleEventRepository: LifecycleEventRepository

  @MockkBean
  lateinit var unhappyVeto: UnhappyVeto

  @MockkBean
  lateinit var resourceStatusService: ResourceStatusService

  @MockkBean
  lateinit var taskTrackingRepository: TaskTrackingRepository

  @MockkBean
  lateinit var diffFingerprintRepository: DiffFingerprintRepository

  @MockkBean
  lateinit var buoyClient: BuoyClient

  @MockkBean
  lateinit var applicationRepository: ApplicationRepository

  @MockkBean
  lateinit var actionRepository: ActionRepository

  @MockkBean
  lateinit var authorizationSupport: AuthorizationSupport

  @MockkBean
  lateinit var keelRepository: KeelRepository

  @MockkBean
  lateinit var artifactRepository: ArtifactRepository

  @MockkBean
  lateinit var front50Service: Front50Service

  @MockkBean
  lateinit var applicationService: ApplicationService

  @MockkBean
  lateinit var deliveryConfigRepository: DeliveryConfigRepository
}
