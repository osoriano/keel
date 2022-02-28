package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsData
import com.netflix.graphql.dgs.DgsDataFetchingEnvironment
import com.netflix.graphql.dgs.DgsMutation
import com.netflix.graphql.dgs.DgsQuery
import com.netflix.graphql.dgs.InputArgument
import com.netflix.graphql.dgs.exceptions.DgsEntityNotFoundException
import com.netflix.spinnaker.keel.application.ApplicationConfig
import com.netflix.spinnaker.keel.application.ApplicationConfig.Companion.DEFAULT_MANIFEST_PATH
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.Front50Service
import com.netflix.spinnaker.keel.front50.model.Application
import com.netflix.spinnaker.keel.graphql.DgsConstants
import com.netflix.spinnaker.keel.graphql.types.MD_Application
import com.netflix.spinnaker.keel.graphql.types.MD_GitIntegration
import com.netflix.spinnaker.keel.graphql.types.MD_UpdateGitIntegrationPayload
import com.netflix.spinnaker.keel.igor.DeliveryConfigImporter
import com.netflix.spinnaker.keel.persistence.ApplicationRepository
import com.netflix.spinnaker.keel.scm.ScmUtils
import com.netflix.spinnaker.keel.upsert.DeliveryConfigUpserter
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.RequestHeader

/**
 * Fetches details about the application's git integration
 */
@DgsComponent
class GitIntegration(
  private val front50Cache: Front50Cache,
  private val scmUtils: ScmUtils,
  private val deliveryConfigUpserter: DeliveryConfigUpserter,
  private val importer: DeliveryConfigImporter,
  private val applicationRepository: ApplicationRepository,
) {

  companion object {
    private val log by lazy { LoggerFactory.getLogger(Front50Cache::class.java) }
  }

  @DgsQuery
  suspend fun md_gitIntegration(dfe: DgsDataFetchingEnvironment, @InputArgument("appName") appName: String): MD_GitIntegration {
    return gitIntegration(appName)
  }

  @DgsData(parentType = DgsConstants.MD_APPLICATION.TYPE_NAME)
  suspend fun gitIntegration(dfe: DgsDataFetchingEnvironment): MD_GitIntegration {
    val app: MD_Application = dfe.getSource()
    return gitIntegration(app.name)
  }

  private suspend fun gitIntegration(application: String): MD_GitIntegration {
    val front50App = front50Cache.applicationByName(application)
    val appConfig = applicationRepository.get(application)
    return toGitIntegration(front50App, appConfig)
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_updateGitIntegration)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  suspend fun updateGitIntegration(
    @InputArgument payload: MD_UpdateGitIntegrationPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): MD_GitIntegration {
    val front50Application = front50Cache.applicationByName(payload.application)
    log.debug("Updating git integration settings of application ${payload.application} by $user. Current: ${front50Application.managedDelivery}, New: $payload")

    val appConfig = ApplicationConfig(
      application = payload.application,
      autoImport = payload.isEnabled == true,
      deliveryConfigPath = payload.manifestPath,
      updatedBy = user
    )
    applicationRepository.store(appConfig)
    return toGitIntegration(front50Application, appConfig)
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_importDeliveryConfig)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #application)"""
  )
  suspend fun importDeliveryConfig(
    @InputArgument application: String,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    val front50App = front50Cache.applicationByName(application, invalidateCache = true)
    val appConfig = applicationRepository.get(application)
    val defaultBranch = scmUtils.getDefaultBranch(front50App)
    val deliveryConfig =
      importer.import(
        front50App.repoType ?: throw DgsEntityNotFoundException("Repo type is undefined for application"),
        front50App.repoProjectKey ?: throw DgsEntityNotFoundException("Repo project is undefined for application"),
        front50App.repoSlug ?: throw DgsEntityNotFoundException("Repo slug is undefined for application"),
        appConfig?.deliveryConfigPath,
        "refs/heads/$defaultBranch"
      )
    deliveryConfigUpserter.upsertConfig(deliveryConfig, allowResourceOverwriting = true, user = user)
    return true
  }

  private fun toGitIntegration(front50App: Application, appConfig: ApplicationConfig?): MD_GitIntegration {
    try {
      scmUtils.getDefaultBranch(front50App)
    } catch (e: Exception) {
      throw DgsEntityNotFoundException("Unable to retrieve your app's git repo details. Please check the app config.")
    }.let { branch ->
      return MD_GitIntegration(
        id = "${front50App.name}-git-integration",
        repository = "${front50App.repoProjectKey}/${front50App.repoSlug}",
        branch = branch,
        isEnabled = appConfig?.autoImport,
        manifestPath = appConfig?.deliveryConfigPath ?: DEFAULT_MANIFEST_PATH,
        link = scmUtils.getBranchLink(front50App.repoType, front50App.repoProjectKey, front50App.repoSlug, branch),
      )
    }
  }
}
