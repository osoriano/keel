package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsData
import com.netflix.graphql.dgs.DgsMutation
import com.netflix.graphql.dgs.InputArgument
import com.netflix.graphql.dgs.exceptions.DgsEntityNotFoundException
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.action.ActionType
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.api.constraints.UpdatedConstraintStatus
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactPin
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVeto
import com.netflix.spinnaker.keel.core.api.PinType
import com.netflix.spinnaker.keel.exceptions.InvalidConstraintException
import com.netflix.spinnaker.keel.graphql.DgsConstants
import com.netflix.spinnaker.keel.graphql.types.MD_Action
import com.netflix.spinnaker.keel.graphql.types.MD_ArtifactVersionActionPayload
import com.netflix.spinnaker.keel.graphql.types.MD_ConstraintStatus
import com.netflix.spinnaker.keel.graphql.types.MD_ConstraintStatusPayload
import com.netflix.spinnaker.keel.graphql.types.MD_DeliveryConfigValidationPayload
import com.netflix.spinnaker.keel.graphql.types.MD_DismissNotificationPayload
import com.netflix.spinnaker.keel.graphql.types.MD_MarkArtifactVersionAsGoodPayload
import com.netflix.spinnaker.keel.graphql.types.MD_PausePayload
import com.netflix.spinnaker.keel.graphql.types.MD_RestartConstraintEvaluationPayload
import com.netflix.spinnaker.keel.graphql.types.MD_RetryArtifactActionPayload
import com.netflix.spinnaker.keel.graphql.types.MD_RollbackToVersionPayload
import com.netflix.spinnaker.keel.graphql.types.MD_ToggleResourceManagementPayload
import com.netflix.spinnaker.keel.graphql.types.MD_UnpinArtifactVersionPayload
import com.netflix.spinnaker.keel.graphql.types.MD_ValidateResult
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.persistence.DismissibleNotificationRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.services.ApplicationService
import de.huxhorn.sulky.ulid.ULID
import org.slf4j.LoggerFactory
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.RequestHeader

/**
 * Component responsible for encapsulating GraphQL mutations exposed on the API.
 */
@DgsComponent
class Mutations(
  private val applicationService: ApplicationService,
  private val actuationPauser: ActuationPauser,
  private val repository: KeelRepository,
  private val notificationRepository: DismissibleNotificationRepository,
) {

  companion object {
    private val log by lazy { LoggerFactory.getLogger(Mutations::class.java) }
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_restartConstraintEvaluation)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun restartConstraintEvaluation(
    @InputArgument payload: MD_RestartConstraintEvaluationPayload,
  ): Boolean {
    val config = repository.getDeliveryConfigForApplication(payload.application)
    if (repository.getConstraintState(config.name, payload.environment, payload.version, payload.type, payload.reference) == null) {
      throw DgsEntityNotFoundException("Constraint ${payload.type} not found for version ${payload.version} of ${payload.reference}")
    }
    return repository.deleteConstraintState(config.name, payload.environment, payload.reference, payload.version, payload.type) > 0
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_updateConstraintStatus)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun updateConstraintStatus(
    @InputArgument payload: MD_ConstraintStatusPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {

    try {
      return applicationService.updateConstraintStatus(
        user = user,
        application = payload.application,
        environment = payload.environment,
        status = payload.toUpdatedConstraintStatus(),
      )
    } catch (e: InvalidConstraintException) {
      throw throw IllegalArgumentException(e.message)
    }
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_toggleManagement)
  @PreAuthorize("@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #application)")
  fun toggleManagement(
    @InputArgument application: String,
    @InputArgument isPaused: Boolean,
    @InputArgument comment: String? = null,
    @InputArgument cancelTasks: Boolean = false,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    if (isPaused) {
      actuationPauser.pauseApplication(application, user, comment, cancelTasks)
    } else {
      actuationPauser.resumeApplication(application, user)
    }
    return true
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_pauseManagement)
  @PreAuthorize("@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #application)")
  fun pauseManagement(
    @InputArgument payload: MD_PausePayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean =
    with(payload) {
      actuationPauser.pauseApplication(application, user, comment, cancelTasks ?: false)
      return true
    }

  @DgsMutation(field = DgsConstants.MUTATION.Md_resumeManagement)
  @PreAuthorize("@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #application)")
  fun resumeManagement(
    @InputArgument application: String,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    actuationPauser.resumeApplication(application, user)
    return true
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_rollbackToArtifactVersion)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun rollbackToVersion(
    @InputArgument payload: MD_RollbackToVersionPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    log.info("Rolling back artifact ${payload.reference} in environment ${payload.environment} of application ${payload.application} from ${payload.fromVersion} to ${payload.toVersion}")
    applicationService.pin(user, payload.application, payload.toEnvironmentArtifactPin())
    if (payload.fromVersion != payload.toVersion) {
      applicationService.markAsVetoedIn(user, payload.application, payload.toEnvironmentArtifactVeto(), true)
    } else {
      log.info("Rolling back but skipping rejection of ${payload.fromVersion} as versions are the same")
    }
    return true
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_lockEnvironmentArtifact)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun lockEnvironmentArtifact(
    @InputArgument payload: MD_ArtifactVersionActionPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    applicationService.pin(user, payload.application, payload.toEnvironmentArtifactPin().copy(type = PinType.LOCK))
    return true
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_pinArtifactVersion)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun pinArtifactVersion(
    @InputArgument payload: MD_ArtifactVersionActionPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    applicationService.pin(user, payload.application, payload.toEnvironmentArtifactPin())
    return true
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_unpinArtifactVersion)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun unpinArtifactVersion(
    @InputArgument payload: MD_UnpinArtifactVersionPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    applicationService.deletePin(
      user = user,
      application = payload.application,
      targetEnvironment = payload.environment,
      reference = payload.reference
    )
    return true
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_markArtifactVersionAsBad)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun markArtifactVersionAsBad(
    @InputArgument payload: MD_ArtifactVersionActionPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    applicationService.markAsVetoedIn(user, payload.application, payload.toEnvironmentArtifactVeto(), true)
    return true
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_markArtifactVersionAsGood)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun markArtifactVersionAsGood(
    @InputArgument payload: MD_MarkArtifactVersionAsGoodPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    applicationService.deleteVeto(
      application = payload.application,
      targetEnvironment = payload.environment,
      reference = payload.reference,
      version = payload.version
    )
    return true
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_retryArtifactVersionAction)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun retryArtifactVersionAction(
    @InputArgument payload: MD_RetryArtifactActionPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): MD_Action {

    val actionType = ActionType.valueOf(payload.actionType.name)
    val newStatus = applicationService.retryArtifactVersionAction(application = payload.application,
                                                                  environment = payload.environment,
                                                                  artifactReference = payload.reference,
                                                                  artifactVersion = payload.version,
                                                                  actionType = actionType,
                                                                  actionId = payload.actionId,
                                                                  user = user)

    ArtifactInEnvironmentContext(
      deliveryConfig = repository.getDeliveryConfigForApplication(payload.application),
      environmentName = payload.environment,
      artifactReference = payload.reference,
      version = payload.version
    ).apply {
      return MD_Action(
        id = this.getMdActionId(actionType, payload.actionId),
        actionId = payload.actionId,
        type = payload.actionId, // Deprecated - TODO: remove this
        actionType = payload.actionType,
        status = newStatus.toDgsActionStatus(),
      )
    }
  }

  /**
   * Dismisses a notification, given it's ID.
   */
  @DgsMutation(field = DgsConstants.MUTATION.Md_dismissNotification)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #payload.application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #payload.application)"""
  )
  fun dismissNotification(
    @InputArgument payload: MD_DismissNotificationPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    log.debug("Dismissing notification with ID=${payload.id} (by user $user)")
    return notificationRepository.dismissNotificationById(payload.application, ULID.parseULID(payload.id), user)
  }

  @DgsMutation(field = DgsConstants.MUTATION.Md_toggleResourceManagement)
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'RESOURCE', #payload.id)
    and @authorizationSupport.hasServiceAccountAccess('RESOURCE', #payload.id)"""
  )
  fun toggleResourceManagement(
    @InputArgument payload: MD_ToggleResourceManagementPayload,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): Boolean {
    if (payload.isPaused) {
      actuationPauser.pauseResource(payload.id, user, payload.comment)
    } else {
      actuationPauser.resumeResource(payload.id, user)
    }
    return true
  }

  @DgsMutation
  fun md_validateDeliveryConfig(
    @InputArgument payload: MD_DeliveryConfigValidationPayload,
  ): MD_ValidateResult {
    try {
      applicationService.validateDeliveryConfig(payload.deliveryConfig)
    } catch (e: Exception) {
      return MD_ValidateResult(success = false, errors = e.message?.let { listOf(it) })
    }
    return MD_ValidateResult(success = true)
  }
}

fun MD_ConstraintStatusPayload.toUpdatedConstraintStatus(): UpdatedConstraintStatus =
  UpdatedConstraintStatus(
    type = type,
    artifactReference = reference,
    artifactVersion = version,
    status = status.toConstraintStatus(),
  )

fun MD_ArtifactVersionActionPayload.toEnvironmentArtifactPin(): EnvironmentArtifactPin =
  EnvironmentArtifactPin(
    targetEnvironment = environment,
    reference = reference,
    version = version,
    comment = comment,
    pinnedBy = null
  )

fun MD_RollbackToVersionPayload.toEnvironmentArtifactPin(): EnvironmentArtifactPin =
  EnvironmentArtifactPin(
    targetEnvironment = environment,
    reference = reference,
    version = toVersion,
    comment = comment,
    type = PinType.ROLLBACK,
    pinnedBy = null
  )

fun MD_RollbackToVersionPayload.toEnvironmentArtifactVeto(): EnvironmentArtifactVeto =
  EnvironmentArtifactVeto(
    targetEnvironment = environment,
    reference = reference,
    version = fromVersion,
    comment = comment,
    vetoedBy = null
  )

fun MD_ArtifactVersionActionPayload.toEnvironmentArtifactVeto(): EnvironmentArtifactVeto =
  EnvironmentArtifactVeto(
    targetEnvironment = environment,
    reference = reference,
    version = version,
    comment = comment,
    vetoedBy = null
  )

fun MD_ConstraintStatus.toConstraintStatus(): ConstraintStatus =
  when (this) {
    MD_ConstraintStatus.FAIL -> ConstraintStatus.FAIL
    MD_ConstraintStatus.FORCE_PASS -> ConstraintStatus.OVERRIDE_PASS
    MD_ConstraintStatus.PASS -> ConstraintStatus.PASS
    MD_ConstraintStatus.PENDING -> ConstraintStatus.PENDING
    else -> throw IllegalArgumentException("Invalid constraint status")
  }
