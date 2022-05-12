package com.netflix.spinnaker.keel.dgs

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsData
import com.netflix.graphql.dgs.DgsMutation
import com.netflix.graphql.dgs.DgsQuery
import com.netflix.graphql.dgs.InputArgument
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.graphql.DgsConstants
import com.netflix.spinnaker.keel.graphql.types.MD_ActuationPlan
import com.netflix.spinnaker.keel.graphql.types.MD_ActuationPlanStatus
import com.netflix.spinnaker.keel.graphql.types.MD_InitiateApplicationMigrationPayload
import com.netflix.spinnaker.keel.graphql.types.MD_Migration
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationReportIssuePayload
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationStatus
import com.netflix.spinnaker.keel.graphql.types.MD_Warning
import com.netflix.spinnaker.keel.graphql.types.MD_Warning_type
import com.netflix.spinnaker.keel.persistence.ArtifactRepository
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.services.ApplicationService
import graphql.execution.DataFetcherResult
import graphql.schema.DataFetchingEnvironment
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.RequestHeader

@DgsComponent
class Migration(
  private val deliveryConfigRepository: DeliveryConfigRepository,
  private val applicationService: ApplicationService,
  private val mapper: ObjectMapper,
  private val artifactRepository: ArtifactRepository
) {

  private val log by lazy { LoggerFactory.getLogger(Migration::class.java) }

  @DgsQuery
  @PreAuthorize("""@authorizationSupport.hasApplicationPermission('READ', 'APPLICATION', #appName)""")
  fun md_migration(dfe: DataFetchingEnvironment, @InputArgument("appName") appName: String): DataFetcherResult<MD_Migration> {
    val status = deliveryConfigRepository.getApplicationMigrationStatus(appName)
    return DataFetcherResult.newResult<MD_Migration>().data(status.toDgs(appName)).localContext(appName).build()
  }

  // get a snapshot of what Keel would do about the config at this moment in time, so we can reassure users
  // we wouldn't mess anything up
  @DgsData(parentType = DgsConstants.MD_MIGRATION.TYPE_NAME)
  fun actuationPlan(dfe: DataFetchingEnvironment): MD_ActuationPlan? {
    val migration = dfe.getSource<MD_Migration>()
    if (migration.status != MD_MigrationStatus.PR_CREATED) {
      return null
    }
    // if no current version for all artifacts in all envs - return null + status
    val appName = dfe.getLocalContext<String>()
    val deliveryConfig = deliveryConfigRepository.getByApplication(appName)

    val completed = deliveryConfig.allArtifactsDeployed()

    val actuationPlan = runBlocking {
      try {
        applicationService.getActuationPlan(deliveryConfig)
      } catch (e: Exception) {
        log.debug("Error calculating actuation plan", e)
        null
      }
    }
    return actuationPlan?.toDgs(completed) ?: MD_ActuationPlan(
      id = "$appName-actuationPlan",
      status = MD_ActuationPlanStatus.FAILED
    )
  }

  @DgsMutation
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun md_migrationReportIssue(
    @InputArgument payload: MD_MigrationReportIssuePayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    val application = payload.application
    runBlocking {
      applicationService.addCommentToJira(application, payload.issue)
    }//TODO[gyardeni]: in the future, cleanup if not needed
    return deliveryConfigRepository.markApplicationMigrationAsBlocked(application, payload.issue, user)
  }

  @DgsMutation
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  suspend fun md_initiateApplicationMigration(
    @InputArgument payload: MD_InitiateApplicationMigrationPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): DataFetcherResult<MD_Migration?> {
    val (prData, prLink) = applicationService.openMigrationPr(
      application = payload.application,
      user = user,
      rawUserGeneratedConfig = payload.deliveryConfig
    )
    // store the delivery config (but paused) so that we can do things with it like diffing resources
    // even before the app is officially onboarded
    applicationService.storePausedMigrationConfig(payload.application, user, prData.deliveryConfig)

    val warning = if (prData.deliveryConfig.artifactWithStatuses) {
      listOf(
        MD_Warning(
          type = MD_Warning_type.TAG_TO_RELEASE_ARTIFACT,
          id = "${payload.application}-tagToReleaseWarning")
      )
    } else null


    return DataFetcherResult.newResult<MD_Migration>().data(
      MD_Migration(
        id = "migration-${payload.application}",
        status = MD_MigrationStatus.PR_CREATED,
        deliveryConfig = mapper.convertValue(prData.autoGeneratedConfig, Map::class.java),
        prLink = prLink,
        warnings = warning,
        userGeneratedDeliveryConfig = mapper.convertValue(prData.userGeneratedConfig, Map::class.java),
      )
    ).localContext(payload.application).build()
  }

  fun DeliveryConfig.allArtifactsDeployed(): Boolean {
    return environments.all { env ->
      artifacts.all { artifact ->
        if (artifact.isUsedIn(env)) {
          artifactRepository.getCurrentlyDeployedArtifactVersionId(this, artifact, env.name) != null
        } else {
          true
        }
      }
    }
  }
}

