package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsData
import com.netflix.graphql.dgs.DgsDataFetchingEnvironment
import com.netflix.graphql.dgs.DgsMutation
import com.netflix.graphql.dgs.InputArgument
import com.netflix.spinnaker.keel.actuation.ExecutionSummaryService
import com.netflix.spinnaker.keel.api.DeployableResourceSpec
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.plugins.DeployableResourceHandler
import com.netflix.spinnaker.keel.api.plugins.ResourceHandler
import com.netflix.spinnaker.keel.api.plugins.supporting
import com.netflix.spinnaker.keel.auth.AuthorizationSupport
import com.netflix.spinnaker.keel.graphql.DgsConstants
import com.netflix.spinnaker.keel.graphql.types.MD_Artifact
import com.netflix.spinnaker.keel.graphql.types.MD_ExecutionSummary
import com.netflix.spinnaker.keel.graphql.types.MD_RecheckResourcePayload
import com.netflix.spinnaker.keel.graphql.types.MD_RedeployResourcePayload
import com.netflix.spinnaker.keel.graphql.types.MD_Resource
import com.netflix.spinnaker.keel.graphql.types.MD_ResourceActuationState
import com.netflix.spinnaker.keel.graphql.types.MD_ResourceTask
import com.netflix.spinnaker.keel.persistence.DiffFingerprintRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.TaskTrackingRepository
import com.netflix.spinnaker.keel.services.ResourceStatusService
import graphql.schema.DataFetchingEnvironment
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.RequestHeader
import retrofit2.HttpException

/**
 * Fetches details about resources, as defined in [schema.graphql]
 */
@DgsComponent
class Resources(
  private val authorizationSupport: AuthorizationSupport,
  private val repository: KeelRepository,
  private val resourceStatusService: ResourceStatusService,
  private val applicationFetcherSupport: ApplicationFetcherSupport,
  private val executionSummaryService: ExecutionSummaryService,
  private val taskTrackingRepository: TaskTrackingRepository,
  private val resourceHandlers: List<ResourceHandler<*, *>>,
  private val diffFingerprintRepository: DiffFingerprintRepository,
) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  @DgsData(parentType = DgsConstants.MD_ARTIFACT.TYPE_NAME, field = DgsConstants.MD_ARTIFACT.Resources)
  fun artifactResources(dfe: DataFetchingEnvironment): List<MD_Resource>? {
    val artifact: MD_Artifact = dfe.getSource()
    val config = applicationFetcherSupport.getDeliveryConfigFromContext(dfe)
    return artifact.environment.let {
      config.resourcesUsing(artifact.reference, artifact.environment).map { it.toDgs(config, artifact.environment) }
    }
  }

  @DgsData(parentType = DgsConstants.MD_RESOURCE.TYPE_NAME, field = DgsConstants.MD_RESOURCE.State)
  fun resourceStatus(dfe: DgsDataFetchingEnvironment): MD_ResourceActuationState {
    val resource: MD_Resource = dfe.getSource()
    val state = resourceStatusService.getActuationState(resource.id)
    return state.toDgs()
  }

  @DgsData(parentType = DgsConstants.MD_RESOURCEACTUATIONSTATE.TYPE_NAME, field = DgsConstants.MD_RESOURCEACTUATIONSTATE.Tasks)
  fun resourceTask(dfe: DgsDataFetchingEnvironment): List<MD_ResourceTask> {
    val resourceState: MD_ResourceActuationState = dfe.getSource()
    val tasks = taskTrackingRepository.getLatestBatchOfTasks(resourceId = resourceState.resourceId)
    return tasks.map { it.toDgs() }
  }

  @DgsData(parentType = DgsConstants.MD_RESOURCETASK.TYPE_NAME, field = DgsConstants.MD_RESOURCETASK.Summary)
  fun taskSummary(dfe: DgsDataFetchingEnvironment): MD_ExecutionSummary? {
    val task: MD_ResourceTask = dfe.getSource()
    try {
      val summary = executionSummaryService.getSummary(task.id)
      return summary?.toDgs()
    } catch (e: HttpException) {
      log.debug("Failed to fetch task ID ${task.id} - $e")
    }
    return null
  }


  @DgsMutation(field = DgsConstants.MUTATION.Md_redeployResource)
  @PreAuthorize("@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)")
  fun redeploy(
    @InputArgument payload: MD_RedeployResourcePayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    val deliveryConfig = repository.deliveryConfigFor(payload.resource)
    val resource = deliveryConfig.resources.find { it.id == payload.resource } as? Resource<DeployableResourceSpec>
      ?: error("Resource ${payload.resource} not found in delivery config for application ${deliveryConfig.application}")
    val environment = deliveryConfig.environmentOfResource(resource)
      ?: error("Environment ${payload.environment} not found in delivery config for application ${deliveryConfig.application}")
    val handler = resourceHandlers.supporting(resource.kind) as? DeployableResourceHandler<DeployableResourceSpec, *>
      ?: error("Compatible resource handler not found for resource ${payload.resource}")

    runBlocking {
      handler.redeploy(deliveryConfig, environment, resource)
    }

    return true
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_recheckResource)
  @PreAuthorize("@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)")
  fun recheckResource(
    @InputArgument payload: MD_RecheckResourcePayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    log.debug("Resource recheck was requested by $user for resource ${payload.resourceId} of application ${payload.application}")
    diffFingerprintRepository.clear(payload.resourceId)
    return true
  }

}
