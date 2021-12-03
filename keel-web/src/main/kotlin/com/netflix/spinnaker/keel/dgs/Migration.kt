package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsData
import com.netflix.graphql.dgs.InputArgument
import com.netflix.spinnaker.keel.api.migration.ApplicationMigrationStatus
import com.netflix.spinnaker.keel.graphql.DgsConstants
import com.netflix.spinnaker.keel.graphql.types.MD_Migration
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationReportIssuePayload
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationStatus
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import graphql.schema.DataFetchingEnvironment
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.RequestHeader

@DgsComponent
class Migration(
  private val deliveryConfigRepository: DeliveryConfigRepository
) {

  @DgsData(parentType = DgsConstants.QUERY.TYPE_NAME, field = DgsConstants.QUERY.Md_migration)
  @PreAuthorize("""@authorizationSupport.hasApplicationPermission('READ', 'APPLICATION', #appName)""")
  fun appMigration(dfe: DataFetchingEnvironment, @InputArgument("appName") appName: String): MD_Migration {
    val status = deliveryConfigRepository.getApplicationMigrationStatus(appName)
    return status.toDgs(appName)
  }

  @DgsData(parentType = DgsConstants.MUTATION.TYPE_NAME, field = DgsConstants.MUTATION.Md_migrationReportIssue)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun reportIssue(
    @InputArgument payload: MD_MigrationReportIssuePayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    // TODO: open a JIRA ticket
    return deliveryConfigRepository.markApplicationMigrationAsBlocked(payload.application, payload.issue, user)
  }
}

fun ApplicationMigrationStatus.toDgs(appName: String) = MD_Migration(
  id = "migration-$appName",
  status = when {
    alreadyManaged -> MD_MigrationStatus.COMPLETED
    isBlocked -> MD_MigrationStatus.BLOCKED
    !isMigratable -> MD_MigrationStatus.NOT_READY
    isMigratable -> MD_MigrationStatus.READY_TO_START
    prCreated -> MD_MigrationStatus.PR_CREATED
    // TODO: add more states
    else -> MD_MigrationStatus.NOT_READY
  },
  deliveryConfig = deliveryConfig
)
