package com.netflix.spinnaker.keel.dgs

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsData
import com.netflix.graphql.dgs.DgsDataFetchingEnvironment
import com.netflix.spinnaker.keel.graphql.DgsConstants
import com.netflix.spinnaker.keel.graphql.types.MD_Config
import com.netflix.spinnaker.keel.igor.DeliveryConfigImporter

/**
 * Fetches details about the application's delivery config
 */
@DgsComponent
class ConfigFetcher(
  private val applicationFetcherSupport: ApplicationFetcherSupport,
  private val yamlMapper: YAMLMapper,
  private val deliveryConfigImporter: DeliveryConfigImporter
) {

  @DgsData(parentType = DgsConstants.MD_APPLICATION.TYPE_NAME, field = DgsConstants.MD_APPLICATION.Config)
  fun config(dfe: DgsDataFetchingEnvironment): MD_Config {
    val config = applicationFetcherSupport.getDeliveryConfigFromContext(dfe)
    return MD_Config(
      id = "${config.application}-${config.name}",
      updatedAt = config.updatedAt,
      rawConfig = config.rawConfig,
      previewEnvironmentsConfigured = config.previewEnvironments.isNotEmpty(),
      isMigrating = config.isMigrating()
    )
  }

  @DgsData(parentType = DgsConstants.MD_CONFIG.TYPE_NAME, field = DgsConstants.MD_CONFIG.ProcessedConfig)
  fun processedConfig(dfe: DgsDataFetchingEnvironment): String? {
    val config = applicationFetcherSupport.getDeliveryConfigFromContext(dfe)
    return yamlMapper.writeValueAsString(config.copy(rawConfig = null))
  }

  @DgsData(parentType = DgsConstants.MD_CONFIG.TYPE_NAME, field = DgsConstants.MD_CONFIG.RawConfig)
  fun rawConfig(dfe: DgsDataFetchingEnvironment): String? {
    val rawConfig = dfe.getSource<MD_Config>().rawConfig
    val config = applicationFetcherSupport.getDeliveryConfigFromContext(dfe)
    // If the raw config is empty or if it was imported via orca (orca adds the gitMetadata to the metadata) we fetch it again from stash
    // TODO: remove this once we removed the import pipeline completely
    return if (rawConfig.isNullOrBlank() || config.metadata.containsKey("gitMetadata")) {
      deliveryConfigImporter.import(config.application, addMetadata = false).rawConfig
    } else {
      rawConfig
    }
  }
}
