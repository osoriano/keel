package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsData
import com.netflix.graphql.dgs.InputArgument
import com.netflix.spinnaker.keel.graphql.DgsConstants
import com.netflix.spinnaker.keel.graphql.types.MD_Migration
import com.netflix.spinnaker.keel.graphql.types.MD_MigrationStatus
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import graphql.schema.DataFetchingEnvironment
import org.springframework.security.access.prepost.PreAuthorize

@DgsComponent
class MigrationFetcher(
  private val delivertConfigRepository: DeliveryConfigRepository
) {

  @DgsData(parentType = DgsConstants.QUERY.TYPE_NAME, field = DgsConstants.QUERY.Md_migration)
  @PreAuthorize("""@authorizationSupport.hasApplicationPermission('READ', 'APPLICATION', #appName)""")
  fun appMigration(dfe: DataFetchingEnvironment, @InputArgument("appName") appName: String): MD_Migration {
    val status = delivertConfigRepository.getApplicationMigrationStatus(appName)
    return MD_Migration(
      id = "migration-$appName",
      status = when {
        !status.isMigratable -> MD_MigrationStatus.NOT_READY
        status.isMigratable -> MD_MigrationStatus.READY_TO_START
        // TODO: add more states
        else -> MD_MigrationStatus.NOT_READY
      },
      deliveryConfig = status.deliveryConfig
    )
  }
}
