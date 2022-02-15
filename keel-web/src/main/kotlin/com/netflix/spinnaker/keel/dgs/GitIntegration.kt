package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsData
import com.netflix.graphql.dgs.DgsDataFetchingEnvironment
import com.netflix.graphql.dgs.DgsMutation
import com.netflix.graphql.dgs.InputArgument
import com.netflix.graphql.dgs.exceptions.DgsEntityNotFoundException
import com.netflix.spinnaker.keel.application.ApplicationConfig
import com.netflix.spinnaker.keel.application.ApplicationConfig.Companion.DEFAULT_MANIFEST_PATH
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.Front50Service
import com.netflix.spinnaker.keel.front50.model.Application
import com.netflix.spinnaker.keel.front50.model.ManagedDeliveryConfig
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
  private val front50Service: Front50Service,
  private val front50Cache: Front50Cache,
  private val scmUtils: ScmUtils,
  private val deliveryConfigUpserter: DeliveryConfigUpserter,
  private val importer: DeliveryConfigImporter,
  private val applicationRepository: ApplicationRepository,
) {

  companion object {
    private val log by lazy { LoggerFactory.getLogger(Front50Cache::class.java) }
  }

  @DgsData(parentType = DgsConstants.MD_APPLICATION.TYPE_NAME)
  fun gitIntegration(dfe: DgsDataFetchingEnvironment): MD_GitIntegration {
    val app: MD_Application = dfe.getSource()
    return runBlocking {
      front50Service.applicationByName(app.name)
    }.toGitIntegration()
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_updateGitIntegration)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun updateGitIntegration(
    @InputArgument payload: MD_UpdateGitIntegrationPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): MD_GitIntegration {
    val front50Application = runBlocking {
      front50Cache.applicationByName(payload.application)
    }
    log.debug("Updating git integration settings of application ${payload.application} by $user. Current: ${front50Application.managedDelivery}, New: $payload")
    val updatedFront50App = runBlocking {
      front50Cache.updateManagedDeliveryConfig(
        front50Application,
        user,
        ManagedDeliveryConfig(
          importDeliveryConfig = payload.isEnabled ?: front50Application.managedDelivery?.importDeliveryConfig ?: false,
          manifestPath = payload.manifestPath ?: front50Application.managedDelivery?.manifestPath
        )
      )
    }
    applicationRepository.store(ApplicationConfig(
      application = payload.application,
      autoImport = payload.isEnabled == true,
      deliveryConfigPath = payload.manifestPath,
      updatedBy = user
    ))
    return updatedFront50App.toGitIntegration()
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_importDeliveryConfig)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #application)"""
  )
  fun importDeliveryConfig(
    @InputArgument application: String,
  ): Boolean {
    val front50App = runBlocking {
      front50Cache.applicationByName(application, invalidateCache = true)
    }
    val defaultBranch = scmUtils.getDefaultBranch(front50App)
    val deliveryConfig =
      importer.import(
        front50App.repoType ?: throw DgsEntityNotFoundException("Repo type is undefined for application"),
        front50App.repoProjectKey ?: throw DgsEntityNotFoundException("Repo project is undefined for application"),
        front50App.repoSlug ?: throw DgsEntityNotFoundException("Repo slug is undefined for application"),
        front50App.managedDelivery?.manifestPath,
        "refs/heads/$defaultBranch"
      )
    deliveryConfigUpserter.upsertConfig(deliveryConfig, allowResourceOverwriting = true)
    return true
  }

  private fun Application.toGitIntegration(): MD_GitIntegration {
    try {
      scmUtils.getDefaultBranch(this)
    } catch (e: Exception) {
      throw DgsEntityNotFoundException("Unable to retrieve your app's git repo details. Please check the app config.")
    }.let { branch ->
      return MD_GitIntegration(
        id = "${name}-git-integration",
        repository = "${repoProjectKey}/${repoSlug}",
        branch = branch,
        isEnabled = managedDelivery?.importDeliveryConfig,
        manifestPath = managedDelivery?.manifestPath ?: DEFAULT_MANIFEST_PATH,
        link = scmUtils.getBranchLink(repoType, repoProjectKey, repoSlug, branch),
      )
    }
  }
}
