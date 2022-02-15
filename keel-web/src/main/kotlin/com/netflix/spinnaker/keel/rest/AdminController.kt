package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.keel.admin.AdminService
import com.netflix.spinnaker.keel.auth.AuthorizationResourceType.SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.auth.AuthorizationSupport
import com.netflix.spinnaker.keel.auth.PermissionLevel.WRITE
import com.netflix.spinnaker.keel.core.api.DEFAULT_SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.export.ExportService
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.Front50Service
import com.netflix.spinnaker.keel.services.ApplicationService
import com.netflix.spinnaker.keel.yaml.APPLICATION_YAML_VALUE
import com.netflix.spinnaker.security.AuthenticatedRequest
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory.getLogger
import org.springframework.http.HttpStatus.NO_CONTENT
import org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import java.time.Duration

@RestController
@RequestMapping(path = ["/poweruser"])
class AdminController(
  private val adminService: AdminService,
  private val exportService: ExportService,
  private val front50Cache: Front50Cache,
  private val front50Service: Front50Service,
  private val authorizationSupport: AuthorizationSupport,
  private val applicationService: ApplicationService
) {
  private val log by lazy { getLogger(javaClass) }

  @DeleteMapping(
    path = ["/applications/{application}"]
  )
  @ResponseStatus(NO_CONTENT)
  fun deleteApplicationData(
    @PathVariable("application") application: String
  ) {
    adminService.deleteApplicationData(application)
  }

  @GetMapping(
    path = ["/applications/paused"]
  )
  fun getPausedApplications() =
    adminService.getPausedApplications()

  @GetMapping(
    path = ["/applications"],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  fun getManagedApplications() =
    adminService.getManagedApplications()

  @PostMapping(
    path = ["/recheck/{resourceId}"]
  )
  fun triggerRecheck(@PathVariable("resourceId") resourceId: String) {
    adminService.triggerRecheck(resourceId)
  }

  @PostMapping(
    path = ["/application/{application}/environment/{environment}/reevaluate"]
  )
  fun forceConstraintReevaluation(
    @PathVariable("application") application: String,
    @PathVariable("environment") environment: String,
    @PathVariable("reference") reference: String,
    @PathVariable("version") version: String,
    @RequestParam("type", required = false) type: String? = null
  ) {
    adminService.forceConstraintReevaluation(application, environment, reference, version, type)
  }

  data class ReferencePayload(
    val reference: String
  )

  /**
   * Force the state of [version] to "SKIPPED"
   *
   * The artifact reference is passed in the body, as a "reference" field,
   * because it may include a slash. By default, tomcat and spring both disallow url-encoded path parameters by default.
   */
  @PostMapping(
    path = ["/application/{application}/environment/{environment}/version/{version}/skip"],
    consumes = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  fun forceSkipArtifactVersion(
    @PathVariable("application") application: String,
    @PathVariable("environment") environment: String,
    @PathVariable("version") version: String,
    @RequestBody payload: ReferencePayload
  ) {
    adminService.forceSkipArtifactVersion(
      application = application,
      environment = environment,
      artifactReference = payload.reference,
      version = version)
  }

  data class ReferenceVerificationPayload(
    val reference: String,
    val verification: String
  )

  /**
   * Force the state of verification with id {verification} in [environment] to OVERRIDE_FAIL
   *
   * The artifact reference and verification iss are passed in the body, as "reference", "verification" fields
   * because they may include slashes. By default, tomcat and spring both disallow url-encoded path parameters by default.
   */
  @PostMapping(
    path = ["/application/{application}/environment/{environment}/version/{version}/fail"],
    consumes = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  fun forceFailVerifications(
    @PathVariable("application") application: String,
    @PathVariable("environment") environment: String,
    @PathVariable("version") version: String,
    @RequestBody payload: ReferenceVerificationPayload

  ) {
    adminService.forceFailVerifications(application, environment, payload.reference, version, payload.verification)
  }

  @PostMapping(
    path = ["/artifacts/metadata/backfill"]
  )
  fun backFillAllArtifactMetadata(
    @RequestParam("age", required = false) age: String?
  ) {
    if (age.isNullOrBlank()) {
      // use default
      adminService.backfillArtifactMetadataAsync(Duration.ofDays(3))
    } else {
      val parsedAge = Duration.parse(age)
      adminService.backfillArtifactMetadataAsync(parsedAge)
    }

  }

  @PostMapping(
    path = ["/application/{application}/disableAllPipelines"]
  )
  fun disableAllPipelines(
    @PathVariable("application") app: String
  ) {
    runBlocking {
      front50Cache.disableAllPipelines(app)
    }
  }


  /**
   * Force a refresh of the the application cache.
   */
  @PostMapping(
    path = ["/cache/applications/refresh"]
  )
  fun refreshApplicationCache() {
    adminService.refreshApplicationCache()
  }

  data class AddAppsToMigrationPayload(
    val apps: List<String>,
    val inAllowedList: Boolean,
  )

  /**
   * Add apps to the list we track for migration.
   * [AddAppsToMigrationPayload.inAllowedList] defines if we ask the app owner to migrate if the app is migratable.
   */
  @PostMapping(
    path = ["/migration/add-apps"]
  )
  fun addAppsToMigrationQueue(
    @RequestBody payload: AddAppsToMigrationPayload
  ) {
    adminService.storeAppForPotentialMigration(payload.apps, payload.inAllowedList)
  }

  /**
   * Run the export
   */
  @PostMapping(
    path = ["/migration/run-mass-export"]
  )
  fun runAppsMassExport() {
    exportService.checkAppsForExport()
  }

  @GetMapping(
    path = ["/taskSummary/{id}"],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  fun getManagedApplications(
    @PathVariable("id") id: String
  ) =
    adminService.getTaskSummary(id)

  @PostMapping(
    path = ["/checkPermissions"],
    produces = [APPLICATION_JSON_VALUE]
  )
  fun getPermissionsErrorMessage(
    @RequestBody body: CheckPermissionBody
  ): CheckPermissionResponse {
    val authorized = authorizationSupport.hasPermission(body.user, body.serviceAccount, SERVICE_ACCOUNT, WRITE)
    if (authorized) {
      return CheckPermissionResponse(authorized = true)
    }

    val serviceAccounts = runBlocking {
      front50Service.getManuallyCreatedServiceAccounts(AuthenticatedRequest.getSpinnakerUser().orElse(DEFAULT_SERVICE_ACCOUNT))
    }
    val serviceAccount = serviceAccounts.find { it.name == body.serviceAccount }

    return if (serviceAccount == null) {
      // could be an auto-created sa, how should we deal with that?
      CheckPermissionResponse(authorized = false, errorMessage = "Service account with name ${body.serviceAccount} doesn't exist, was it automatically created?")
    } else {
      CheckPermissionResponse(authorized = false, errorMessage = "User ${body.user} must have access to all these groups: ${serviceAccount.memberOf}. Request access in go/accessui.")
    }
  }

  @GetMapping(
    path = ["/applications/{application}/plan"],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  fun getActuationPlan(@PathVariable application: String) =
    runBlocking { applicationService.getActuationPlan(application) }

  @PostMapping(
    path = ["/sync-front50-config"]
  )
  fun syncFront50Config() {
    adminService.syncFront50Config()
  }

}

data class CheckPermissionBody(
  val user: String,
  val serviceAccount: String
)

data class CheckPermissionResponse(
  val authorized: Boolean,
  val errorMessage: String? = null
)
