package com.netflix.spinnaker.keel.dgs

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsMutation
import com.netflix.graphql.dgs.DgsQuery
import com.netflix.graphql.dgs.InputArgument
import com.netflix.spinnaker.keel.graphql.types.MD_InitiateApplicationMigrationPayload
import com.netflix.spinnaker.keel.graphql.types.MD_Migration
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationReportIssuePayload
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationStatus
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.services.ApplicationService
import graphql.schema.DataFetchingEnvironment
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.RequestHeader

@DgsComponent
class Migration(
  private val deliveryConfigRepository: DeliveryConfigRepository,
  private val applicationService: ApplicationService,
  private val mapper: ObjectMapper
) {

  private val log by lazy { LoggerFactory.getLogger(Migration::class.java) }

  @DgsQuery
  @PreAuthorize("""@authorizationSupport.hasApplicationPermission('READ', 'APPLICATION', #appName)""")
  fun md_migration(dfe: DataFetchingEnvironment, @InputArgument("appName") appName: String): MD_Migration {
    val status = deliveryConfigRepository.getApplicationMigrationStatus(appName)
    return status.toDgs(appName)
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
    // TODO: open a JIRA ticket
    return deliveryConfigRepository.markApplicationMigrationAsBlocked(payload.application, payload.issue, user)
  }

  @DgsMutation
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun md_initiateApplicationMigration(
    @InputArgument payload: MD_InitiateApplicationMigrationPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): MD_Migration? {
    val (prData, prLink) = runBlocking {
      applicationService.openMigrationPr(
        application = payload.application,
        user = user
      )
    }

    // store the delivery config (but paused) so that we can do things with it like diffing resources
    // even before the app is officially onboarded
    applicationService.storePausedMigrationConfig(payload.application, user)

    // get a snapshot of what Keel would do about the config at this moment in time, so we can reassure users
    // we wouldn't mess anything up
    val actuationPlan = runBlocking {
      try {
        applicationService.getActuationPlan(payload.application)
      } catch (e: Exception) {
        log.debug("Error calculating actuation plan", e)
        null
      }
    }

    return MD_Migration(
      id = "migration-${payload.application}",
      status = MD_MigrationStatus.PR_CREATED,
      deliveryConfig = mapper.convertValue(prData.deliveryConfig, Map::class.java),
      prLink = prLink,
      actuationPlan = actuationPlan?.toDgs()
    )
  }
}
