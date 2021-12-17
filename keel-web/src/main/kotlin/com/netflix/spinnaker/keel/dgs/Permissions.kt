package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsData
import com.netflix.graphql.dgs.DgsDataFetchingEnvironment
import com.netflix.spinnaker.keel.auth.AuthorizationSupport
import com.netflix.spinnaker.keel.core.api.DEFAULT_SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.front50.Front50Service
import com.netflix.spinnaker.keel.graphql.DgsConstants
import com.netflix.spinnaker.keel.graphql.types.MD_Application
import com.netflix.spinnaker.keel.graphql.types.MD_UserPermissions
import com.netflix.spinnaker.security.AuthenticatedRequest
import kotlinx.coroutines.runBlocking
import org.springframework.web.bind.annotation.RequestHeader

@DgsComponent
class Permissions(
  private val authorizationSupport: AuthorizationSupport,
  private val front50Service: Front50Service
) {

  @DgsData(parentType = DgsConstants.MD_APPLICATION.TYPE_NAME)
  fun userPermissions(
    dfe: DgsDataFetchingEnvironment,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): MD_UserPermissions {
    val application: MD_Application = dfe.getSource()
    val appPermission = authorizationSupport.hasApplicationPermission("WRITE", "APPLICATION", application.name)
    val serviceAccountPermissions = authorizationSupport.hasServiceAccountAccess(application.account)
    val id = "userPermissions-${application.name}"
    return when {
      !appPermission -> MD_UserPermissions(id = id, writeAccess = false, error = "User does not have write permission to ${application.name}")
      !serviceAccountPermissions -> {
        val serviceAccounts = runBlocking {
          front50Service.getManuallyCreatedServiceAccounts(
            AuthenticatedRequest.getSpinnakerUser().orElse(DEFAULT_SERVICE_ACCOUNT))
        }
        val serviceAccount = serviceAccounts.find { it.name == application.account }
        if (serviceAccount != null) {
          MD_UserPermissions(
            id = id,
            writeAccess = false,
            error = "User must have access to all the groups of service account $serviceAccount: ${serviceAccount.memberOf}. Request access in go/accessui, or by reaching out to #security-help."
          )
        } else {
          MD_UserPermissions(
            id = id,
            writeAccess = false,
            error = "Service account was automatically created. User must be a member of the google group ${application.account}. Check go/gg for membership info, and reach out to #security-help if you can't get access."
          )
        }
      }
      else -> MD_UserPermissions(id = id, writeAccess = true)
    }
  }
}
