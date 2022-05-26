package com.netflix.spinnaker.keel.dgs

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsDataFetchingEnvironment
import com.netflix.graphql.dgs.DgsQuery
import com.netflix.graphql.dgs.InputArgument
import com.netflix.spinnaker.keel.graphql.types.MD_ActuationPlan
import com.netflix.spinnaker.keel.jackson.readValueInliningAliases
import com.netflix.spinnaker.keel.services.ApplicationService

/**
 * Provides a GraphQL query to perform a "dry-run" of the provided delivery config.
 */
@DgsComponent
class DryRun(
  private val applicationService: ApplicationService,
  private val yamlMapper: YAMLMapper
) {

  @DgsQuery(field = "md_dryRun")
  suspend fun dryRun(
    dfe: DgsDataFetchingEnvironment,
    @InputArgument("submittedDeliveryConfig") submittedDeliveryConfig: String
  ): MD_ActuationPlan {
    return applicationService.dryRun(
      submittedDeliveryConfig.let { yamlMapper.readValueInliningAliases(it) }
    ).toDgs(completed = true)
  }
}
