package com.netflix.spinnaker.keel.auth

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.fiat.shared.FiatPermissionEvaluator
import com.netflix.spinnaker.fiat.shared.triggers.ManagedDeliveryTriggerIdentity
import com.netflix.spinnaker.keel.api.AccountAwareLocations
import com.netflix.spinnaker.keel.api.Locatable
import com.netflix.spinnaker.keel.auth.AuthorizationSupport.TargetEntity.APPLICATION
import com.netflix.spinnaker.keel.auth.AuthorizationSupport.TargetEntity.DELIVERY_CONFIG
import com.netflix.spinnaker.keel.auth.AuthorizationSupport.TargetEntity.RESOURCE
import com.netflix.spinnaker.keel.auth.AuthorizationSupport.TargetEntity.SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.NoSuchEntityException
import com.netflix.spinnaker.kork.dynamicconfig.DynamicConfigService
import com.netflix.spinnaker.kork.web.exceptions.InvalidRequestException
import com.netflix.spinnaker.security.AuthenticatedRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.security.access.AccessDeniedException
import org.springframework.security.core.Authentication
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.stereotype.Component

/**
 * Support for authorization of API calls.
 *
 * @link https://github.com/spinnaker/keel/blob/master/docs/authorization.md
 */
@Component
class AuthorizationSupport(
  private val permissionEvaluator: FiatPermissionEvaluator,
  private val repository: KeelRepository,
  private val dynamicConfigService: DynamicConfigService
) {
  val log: Logger by lazy { LoggerFactory.getLogger(javaClass) }

  enum class TargetEntity {
    APPLICATION, DELIVERY_CONFIG, RESOURCE, SERVICE_ACCOUNT;
    override fun toString() = name.lowercase()
  }

  private fun enabled() = dynamicConfigService.isEnabled("keel.authorization", true)

  /**
   * @return true if a user has the permission to access the resource at the level requested
   *
   * Use this function if you are checking authorization and you have the email instead of grabbing the requester from
   * the spring auth context.
   */
  fun hasPermission(email: String, resourceName: String, resourceType: AuthorizationResourceType, authorization: PermissionLevel): Boolean {
    return permissionEvaluator.hasPermission(email, resourceName, resourceType.name.lowercase(), authorization.name)
  }

  /**
   * Returns true if the caller has the specified permission (action) to access the application associated with the
   * specified target object.
   */
  fun hasApplicationPermission(action: String, target: String, identifier: String) =
    passes { checkApplicationPermission(PermissionLevel.valueOf(action), TargetEntity.valueOf(target), identifier) }

  /**
   * Returns true if  the caller has access to the specified service account.
   */
  fun hasServiceAccountAccess(target: String, identifier: String) =
    passes { checkServiceAccountAccess(TargetEntity.valueOf(target), identifier) }

  /**
   * Returns true if  the caller has access to the specified service account.
   */
  fun hasServiceAccountAccess(serviceAccount: String?) =
    serviceAccount?.let { passes { checkServiceAccountAccess(SERVICE_ACCOUNT, it) } } ?: true

  /**
   * Returns true if the caller has the specified permission (action) to access the cloud account associated with the
   * specified target object.
   */
  fun hasCloudAccountPermission(action: String, target: String, identifier: String) =
    passes { checkCloudAccountPermission(PermissionLevel.valueOf(action), TargetEntity.valueOf(target), identifier) }

  /**
   * Verifies that the caller has the specified permission (action) to access the application associated with the
   * specified target object.
   *
   * @throws AccessDeniedException if caller does not have the required permission.
   */
  fun checkApplicationPermission(action: PermissionLevel, target: TargetEntity, identifier: String) {
    if (!enabled()) return

    withAuthentication(target, identifier) { auth ->
      val application = when (target) {
        RESOURCE -> repository.getResource(identifier).application
        APPLICATION -> identifier
        DELIVERY_CONFIG -> repository.getDeliveryConfig(identifier).application
        else -> throw InvalidRequestException("Invalid target type ${target.name} for application permission check")
      }
      checkPermission(auth, application, AuthorizationResourceType.APPLICATION, action.name)
    }
  }

  /**
   * Verifies that the caller has access to the specified service account.
   *
   * @throws AccessDeniedException if caller does not have the required permission.
   */
  fun checkServiceAccountAccess(target: TargetEntity, identifier: String) {
    if (!enabled()) return

    withAuthentication(target, identifier) { auth ->
      val serviceAccount = when (target) {
        SERVICE_ACCOUNT -> identifier
        RESOURCE -> repository.getResource(identifier).serviceAccount
        APPLICATION -> repository.getDeliveryConfigForApplication(identifier).serviceAccount
        DELIVERY_CONFIG -> repository.getDeliveryConfig(identifier).serviceAccount
      }
      checkPermission(auth, serviceAccount, AuthorizationResourceType.SERVICE_ACCOUNT, "ACCESS")
    }
  }

  /**
   * Verifies that the caller has the specified permission to all applicable resources (i.e. resources whose specs
   * are [Locatable]) identified by the target type and identifier, as follows:
   *   - If target is RESOURCE, check the resource itself
   *   - If target is DELIVERY_CONFIG, check all the resources in all the environments of the delivery config
   *   - If target is APPLICATION, do the same as for DELIVERY_CONFIG
   *
   * @throws AccessDeniedException if caller does not have the required permission.
   */
  fun checkCloudAccountPermission(action: PermissionLevel, target: TargetEntity, identifier: String) {
    if (!enabled()) return

    withAuthentication(target, identifier) { auth ->
      val locatableResources = when (target) {
        RESOURCE -> listOf(repository.getResource(identifier))
        APPLICATION -> repository.getDeliveryConfigForApplication(identifier).resources
        DELIVERY_CONFIG -> repository.getDeliveryConfig(identifier).resources
        else -> throw InvalidRequestException("Invalid target type ${target.name} for cloud account permission check")
      }.filter {
        // pick only locatable resources that have account information
        (it.spec as? Locatable<*>)?.locations is AccountAwareLocations<*>
      }

      locatableResources.forEach {
        val locations = (it.spec as Locatable<AccountAwareLocations<*>>).locations
        val account = locations.account
        checkPermission(auth, account, AuthorizationResourceType.ACCOUNT, action.name)
      }
    }
  }

  private fun withAuthentication(target: TargetEntity, identifier: String, block: (Authentication) -> Unit) {
    try {
      val auth = SecurityContextHolder.getContext().authentication
      block(auth)
    } catch (e: NoSuchEntityException) {
      // If entity doesn't exist return true so a 404 is returned from the controller.
      log.debug("${target.name} $identifier not found. Allowing request to return 404.")
    }
  }

  /**
   * Ensures the user (as determined by the current authentication context) has the specified permission to the
   * specified resource.
   */
  fun checkPermission(authentication: Authentication, resourceName: String, resourceType: AuthorizationResourceType, permission: String) {
    val user = AuthenticatedRequest.getSpinnakerUser().orElse("unknown")
    log.debug("Checking $permission permission to $resourceType $resourceName for user $user (details: $authentication")
    checkPermission(user, resourceName, resourceType, permission)
  }

  /**
   * Ensures the [user] has the specified permission to the specified resource.
   */
  fun checkPermission(user: String, resourceName: String, resourceType: AuthorizationResourceType, permission: String) {
    val allowed = AuthenticatedRequest.allowAnonymous {
      permissionEvaluator.hasPermission(user, resourceName, resourceType.name, permission)
    }

    log.debug("[ACCESS ${allowed.toAuthorization()}] User $user: $permission permission to $resourceType $resourceName.")
    if (!allowed) {
      throw AccessDeniedException(
        "User $user does not have ${permission.humanFriendly()} permission to ${resourceType.name.humanFriendly()} $resourceName")
    }
  }

  private fun passes(authorizationCheck: () -> Unit) =
    try {
      authorizationCheck()
      true
    } catch (e: AccessDeniedException) {
      false
    }

  private fun Boolean.toAuthorization() = if (this) "ALLOWED" else "DENIED"

  private fun String.humanFriendly() = this.lowercase().replace('_', ' ')

  companion object {
    private val objectMapper: ObjectMapper = ObjectMapper()

    /**
     * @return A token appropriate for setting the  X-SPINNAKER-AUTH-TOKEN request header
     * for the specified [application] and [legacyRunAs] user.
     */
    fun getSpinnakerAuthToken(application: String, legacyRunAs: String): String {
      return ManagedDeliveryTriggerIdentity().let {
        it.applicationName = application
        it.legacyRunAs = legacyRunAs
        "object:" + objectMapper.writeValueAsString(it)
      }
    }
  }
}
