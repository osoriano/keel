package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsData
import com.netflix.graphql.dgs.DgsDataFetchingEnvironment
import com.netflix.spinnaker.keel.actuation.ExecutionSummaryService
import com.netflix.spinnaker.keel.artifacts.ArtifactVersionLinks
import com.netflix.spinnaker.keel.auth.AuthorizationSupport
import com.netflix.spinnaker.keel.graphql.DgsConstants
import com.netflix.spinnaker.keel.graphql.types.MD_Artifact
import com.netflix.spinnaker.keel.graphql.types.MD_ExecutionSummary
import com.netflix.spinnaker.keel.graphql.types.MD_Resource
import com.netflix.spinnaker.keel.graphql.types.MD_ResourceActuationState
import com.netflix.spinnaker.keel.graphql.types.MD_ResourceTask
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.persistence.DismissibleNotificationRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.TaskTrackingRepository
import com.netflix.spinnaker.keel.scm.ScmUtils
import com.netflix.spinnaker.keel.services.ResourceStatusService
import graphql.schema.DataFetchingEnvironment
import org.slf4j.LoggerFactory
import retrofit2.HttpException

/**
 * Fetches details about resources, as defined in [schema.graphql]
 */
@DgsComponent
class ResourceFetcher(
  private val authorizationSupport: AuthorizationSupport,
  private val keelRepository: KeelRepository,
  private val resourceStatusService: ResourceStatusService,
  private val actuationPauser: ActuationPauser,
  private val artifactVersionLinks: ArtifactVersionLinks,
  private val applicationFetcherSupport: ApplicationFetcherSupport,
  private val notificationRepository: DismissibleNotificationRepository,
  private val scmUtils: ScmUtils,
  private val executionSummaryService: ExecutionSummaryService,
  private val taskTrackingRepository: TaskTrackingRepository
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
      return summary.toDgs()
    } catch (e: HttpException) {
      log.debug("Failed to fetch task ID ${task.id} - $e")
    }
    return null
  }

}
