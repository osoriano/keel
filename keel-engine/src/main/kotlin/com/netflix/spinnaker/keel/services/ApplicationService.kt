package com.netflix.spinnaker.keel.services

import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.config.ArtifactConfig
import com.netflix.spinnaker.keel.actuation.EnvironmentTaskCanceler
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.JiraBridge
import com.netflix.spinnaker.keel.api.Locatable
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.ResourceDiff
import com.netflix.spinnaker.keel.api.ResourceDiffFactory
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.StashBridge
import com.netflix.spinnaker.keel.api.action.ActionType
import com.netflix.spinnaker.keel.api.constraints.ConstraintState
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.api.constraints.UpdatedConstraintStatus
import com.netflix.spinnaker.keel.api.jira.JiraComment
import com.netflix.spinnaker.keel.api.jira.JiraComponent
import com.netflix.spinnaker.keel.api.jira.JiraFields
import com.netflix.spinnaker.keel.api.jira.JiraIssue
import com.netflix.spinnaker.keel.api.migration.MigrationCommitData
import com.netflix.spinnaker.keel.api.plugins.ResourceHandler
import com.netflix.spinnaker.keel.api.plugins.supporting
import com.netflix.spinnaker.keel.core.api.ActuationPlan
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactPin
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVeto
import com.netflix.spinnaker.keel.core.api.EnvironmentPlan
import com.netflix.spinnaker.keel.core.api.PromotionStatus
import com.netflix.spinnaker.keel.core.api.ResourceAction
import com.netflix.spinnaker.keel.core.api.ResourceAction.CREATE
import com.netflix.spinnaker.keel.core.api.ResourceAction.NONE
import com.netflix.spinnaker.keel.core.api.ResourceAction.UPDATE
import com.netflix.spinnaker.keel.core.api.ResourcePlan
import com.netflix.spinnaker.keel.core.api.ResourceSummary
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.events.MarkAsBadNotification
import com.netflix.spinnaker.keel.events.MigrationReadyNotification
import com.netflix.spinnaker.keel.events.PinnedNotification
import com.netflix.spinnaker.keel.events.UnpinnedNotification
import com.netflix.spinnaker.keel.exceptions.InvalidConstraintException
import com.netflix.spinnaker.keel.exceptions.InvalidVetoException
import com.netflix.spinnaker.keel.logging.blankMDC
import com.netflix.spinnaker.keel.logging.withThreadTracingContext
import com.netflix.spinnaker.keel.migrations.ApplicationPrData
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.persistence.ArtifactNotFoundException
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.NoDeliveryConfigForApplication
import com.netflix.spinnaker.keel.persistence.NoSuchDeliveryConfigException
import com.netflix.spinnaker.keel.persistence.PausedRepository
import com.netflix.spinnaker.keel.scheduling.ResourceSchedulerService
import com.netflix.spinnaker.keel.upsert.DeliveryConfigUpserter
import com.netflix.spinnaker.keel.validators.DeliveryConfigValidator
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationEventPublisher
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Propagation.REQUIRED
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.ResponseStatus
import java.time.Clock
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import org.springframework.core.env.Environment as SpringEnvironment

/**
 * Service object that offers high-level APIs for application-related operations.
 */
@Component
@EnableConfigurationProperties(ArtifactConfig::class)
class ApplicationService(
  private val repository: KeelRepository,
  private val resourceStatusService: ResourceStatusService,
  private val publisher: ApplicationEventPublisher,
  private val springEnv: SpringEnvironment,
  private val clock: Clock,
  private val spectator: Registry,
  private val environmentTaskCanceler: EnvironmentTaskCanceler,
  private val yamlMapper: YAMLMapper,
  private val objectMapper: ObjectMapper,
  private val stashBridge: StashBridge,
  private val jiraBridge: JiraBridge,
  private val pausedRepository: PausedRepository,
  private val handlers: List<ResourceHandler<*, *>>,
  private val diffFactory: ResourceDiffFactory,
  private val deliveryConfigUpserter: DeliveryConfigUpserter,
  private val actuationPauser: ActuationPauser,
  private val resourceSchedulerService: ResourceSchedulerService,
  private val validator: DeliveryConfigValidator,
) : CoroutineScope {
  override val coroutineContext: CoroutineContext = Dispatchers.Default

  companion object {
    //attributes that should be stripped before being returned through the api
    val privateConstraintAttrs = listOf("manual-judgement")
  }

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  private val now: Instant
    get() = clock.instant()

  private val RESOURCE_SUMMARY_CONSTRUCT_DURATION_ID = "keel.api.resource.summary.duration"
  private val APPLICATION_ACTUATION_PLAN_DURATION_ID = "keel.api.application.actuation.plan.duration"

  fun hasManagedResources(application: String) = repository.hasManagedResources(application)

  fun getDeliveryConfig(application: String) = repository.getDeliveryConfigForApplication(application)

  fun deleteDeliveryConfig(name: String) {
    val config = repository.getDeliveryConfig(name)
    repository.deleteDeliveryConfigByName(name)
    stopSchedulingAllResources(config)
  }

  fun deleteConfigByApp(application: String) {
    launch(blankMDC) {
      try {
        log.debug("Deleting delivery config for application $application")
        val config = getDeliveryConfig(application)
        repository.deleteDeliveryConfigByApplication(application)
        stopSchedulingAllResources(config)
      } catch (ex: NoDeliveryConfigForApplication) {
        log.info("attempted to delete delivery config for app that doesn't have a config: $application")
      }
    }
  }

  /**
   * Makes sure that temporal is no longer scheduling checks on the deleted resources
   */
  private fun stopSchedulingAllResources(config: DeliveryConfig) {
    log.debug("Stopping scheduling of resources ${config.resources.map { it.id }} due to config deletion for application ${config.application}")
    config.resources.forEach { resource ->
      resourceSchedulerService.stopScheduling(resource)
    }
  }

  fun updateConstraintStatus(user: String, application: String, environment: String, status: UpdatedConstraintStatus): Boolean {
    val config = repository.getDeliveryConfigForApplication(application)
    val constraintName = "${config.name}/$environment/${status.type}/${status.artifactVersion}"
    val artifactReference = status.artifactReference

    val artifact = config.matchingArtifactByReference(artifactReference)
      ?: throw InvalidConstraintException(constraintName, "artifact not found in delivery config for reference $artifactReference")

    return withThreadTracingContext(artifact, status.artifactVersion) {
      val currentState = repository.getConstraintState(config.name, environment, status.artifactVersion, status.type, artifactReference)
        ?: throw InvalidConstraintException(constraintName, "constraint not found")

      val newState = currentState.copy(
        status = status.status,
        comment = status.comment ?: currentState.comment,
        judgedAt = Instant.now(),
        judgedBy = user
      )

      repository.storeConstraintState(newState)
      repository.triggerDeliveryConfigRecheck(config) // recheck environments to fast track a deployment

      currentState.status != newState.status
    }
  }

  fun pin(user: String, application: String, pin: EnvironmentArtifactPin) {
    log.info("Pinning application $application by user $user: {}", pin)
    val config = repository.getDeliveryConfigForApplication(application)
    repository.pinEnvironment(config, pin.copy(pinnedBy = user))
    environmentTaskCanceler.cancelTasksForPin(application, pin, user)
    repository.triggerDeliveryConfigRecheck(config) // recheck environments to reflect pin immediately
    publisher.publishEvent(PinnedNotification(config, pin.copy(pinnedBy = user)))
  }

  fun deletePin(user: String, application: String, targetEnvironment: String, reference: String? = null) {
    log.info("Removing pin for application $application by user $user in environment $targetEnvironment for artifact reference $reference")
    val config = repository.getDeliveryConfigForApplication(application)
    val pinnedEnvironment = repository.pinnedEnvironments(config).find { it.targetEnvironment == targetEnvironment }
    repository.deletePin(config, targetEnvironment, reference)
    repository.triggerDeliveryConfigRecheck(config) // recheck environments to reflect pin removal immediately

    publisher.publishEvent(UnpinnedNotification(config,
      pinnedEnvironment,
      targetEnvironment,
      user))
  }

  fun markAsVetoedIn(user: String, application: String, veto: EnvironmentArtifactVeto, force: Boolean) {
    log.info("Vetoing ${veto.version} for application $application by user $user in environment ${veto.targetEnvironment} for artifact reference ${veto.reference}")
    val config = repository.getDeliveryConfigForApplication(application)
    val succeeded = repository.markAsVetoedIn(
      deliveryConfig = config,
      veto = veto.copy(vetoedBy = user),
      force = force
    )
    if (!succeeded) {
      throw InvalidVetoException(application, veto.targetEnvironment, veto.reference, veto.version)
    }
    log.info("Successfully marked artifact version ${veto.reference}: ${veto.version} of application $application as bad")
    environmentTaskCanceler.cancelTasksForVeto(application, veto, user)
    repository.triggerDeliveryConfigRecheck(config) // recheck environments to reflect veto immediately
    publisher.publishEvent(MarkAsBadNotification(
      config = config,
      user = user,
      veto = veto.copy(vetoedBy = user)
    ))
  }

  fun deleteVeto(application: String, targetEnvironment: String, reference: String, version: String) {
    log.info("Removing veto of $version for application $application in environment $targetEnvironment for artifact reference $reference")
    val config = repository.getDeliveryConfigForApplication(application)
    val artifact = config.matchingArtifactByReference(reference)
      ?: throw ArtifactNotFoundException(reference, config.name)
    repository.deleteVeto(
      deliveryConfig = config,
      artifact = artifact,
      version = version,
      targetEnvironment = targetEnvironment
    )
    repository.triggerDeliveryConfigRecheck(config) // recheck environments to reflect removed veto immediately
  }

  /**
   * Returns a list of [ResourceSummary] for the specified application.
   */
  fun getResourceSummariesFor(application: String): List<ResourceSummary> {
    return try {
      val startTime = now
      val deliveryConfig = repository.getDeliveryConfigForApplication(application)
      val summaries = getResourceSummaries(deliveryConfig)
      spectator.timer(
        RESOURCE_SUMMARY_CONSTRUCT_DURATION_ID,
        listOf(BasicTag("application", application))
      ).record(Duration.between(startTime, now))
      summaries
    } catch (e: NoSuchDeliveryConfigException) {
      emptyList()
    }
  }

  fun getResourceSummaries(deliveryConfig: DeliveryConfig): List<ResourceSummary> =
    deliveryConfig.resources.map { resource ->
      resource.toResourceSummary(deliveryConfig)
    }

  fun Resource<*>.toResourceSummary(deliveryConfig: DeliveryConfig) =
    ResourceSummary(
      resource = this,
      status = resourceStatusService.getStatus(id),
      locations = if (spec is Locatable<*>) {
        (spec as Locatable<*>).locations
      } else {
        null
      },
      pause = actuationPauser.getResourcePauseInfo(id)
    )

  fun getApplicationEventHistory(application: String, limit: Int) =
    repository.applicationEventHistory(application, limit)

  fun retryArtifactVersionAction(application: String, environment: String, artifactReference: String, artifactVersion: String, actionType: ActionType, actionId: String, user: String): ConstraintStatus {
    ArtifactInEnvironmentContext(
      deliveryConfig = repository.getDeliveryConfigForApplication(application),
      environmentName = environment,
      artifactReference = artifactReference,
      version = artifactVersion
    ).run {
      val action = action(actionType, actionId) ?: throw InvalidActionId(actionId, this)
      repository.getActionState(
        context = this,
        action = action
      )?.run {
        if (!status.complete) throw ActionIncomplete()
      }
      return repository.resetActionState(context = this, action = action, user = user)
    }
  }

  fun parseAndValidateDeliveryConfig(rawDeliveryConfig: Any): SubmittedDeliveryConfig {
    return objectMapper.convertValue<SubmittedDeliveryConfig>(rawDeliveryConfig).also {
      validator.validate(it)
    }
  }

  /**
   * This function is fetching application's data like config, repo and project name
   * And then calling stash to create commit and open a pull request with the application's delivery config file
   * if [userEditedDeliveryConfig] is not null, we'll store it as a user edited config and use it to open/update a PR
   * Otherwise, we take the config that is stored in the DB
   */
  suspend fun openMigrationPr(application: String, user: String, userEditedDeliveryConfig: Any? = null): Pair<ApplicationPrData, String> {
    if (userEditedDeliveryConfig != null) {
      val tempConfig = objectMapper.convertValue<SubmittedDeliveryConfig>(userEditedDeliveryConfig)
      validator.validate(tempConfig)
      repository.storeUserGeneratedConfigForMigratedApplication(application, tempConfig.copy(
        metadata = mapOf(
          DeliveryConfig.MIGRATING_KEY to true
        )
      ))
    }

    val applicationPrData = repository.getMigratableApplicationData(application)

    //sending the exported config in a yml format, as string
    val configAsString = yamlMapper.writeValueAsString(applicationPrData.deliveryConfig)

    val migrationCommitData = MigrationCommitData(
      fileContents = configAsString,
      user = user,
      repoSlug = applicationPrData.repoSlug,
      projectKey = applicationPrData.projectKey
    )
    //if we already have a PR link, add a new commit
    val prLink = if (applicationPrData.prLink != null) {
      try {
        stashBridge.addCommitForExistingPR(migrationCommitData)
        applicationPrData.prLink
      } catch (ex: Exception) {
        log.debug("failed to add a commit and update pull request ${applicationPrData.prLink} for application $application.", ex)
        throw ex
      }
    } else {
      try {
        stashBridge.createCommitAndPrFromConfig(migrationCommitData)
      } catch (ex: Exception) {
        log.debug("failed to created pull request for application $application.", ex)
        throw ex
      }
    }

    log.debug("Migration PR created for application $application: $prLink")

    //storing the pr link
    repository.storePrLinkForMigratedApplication(application, prLink!!)

    //create a jira issue
    try {
      val jiraRequest = JiraIssue(
        JiraFields(
          summary = "Managed Delivery migration started for application $application",
          description = "This is an automated ticket. A PR was created to migrate application $application to Managed Delivery.\n Check out the PR: $prLink",
          components = listOf(JiraComponent(name = "migration"))
        )
      )
      log.debug("trying to create a jira issue with request $jiraRequest for application $application")
      val jiraIssue = jiraBridge.createIssue(jiraRequest)
      log.debug("jira issue ${jiraIssue.key} was created for application $application migration PR")
      repository.storeJiraLinkForMigratedApplication(application, jiraIssue.self)
    } catch (ex: Exception) {
      log.debug("tried to create a new jira issue for a migration PR for application $application, but caught an exception", ex.message
        ?: ex)
    }

    return Pair(applicationPrData, prLink)
  }

  suspend fun addCommentToJira(application: String, issue: String) {
    val jiraLink = repository.getApplicationMigrationStatus(application = application).jiraLink
    if (jiraLink != null) {
      try {
        runBlocking {
          jiraBridge.addComment(jiraLink.substringAfterLast('/'), JiraComment(body = issue))
        }
      } catch (ex: Exception) {
        log.debug("failed to add a jira comment for application $application with jira link $jiraLink", ex)
      }
    } else {
      log.debug("no jira link found for application $application; Can't add comment with issue $issue")
    }
  }

  /**
   * Stores the [DeliveryConfig] for the specified [application] after pausing it, such that we don't take action on the
   * config. This is so that we can interact with the config in the database like a normal application (including
   * diffing resources against current state) in support of the migration wizard.
   */
  @Transactional(propagation = REQUIRED)
  fun storePausedMigrationConfig(application: String, user: String, deliveryConfig: SubmittedDeliveryConfig): DeliveryConfig {
    pausedRepository.pauseApplication(application, user, "Automatically paused while upgrading to Managed Delivery.")
    return deliveryConfigUpserter.upsertConfig(
      deliveryConfig,
      allowResourceOverwriting = true,
      user = user
    ).first
  }

  /**
   * Returns the current point-in-time [ActuationPlan] for the specified [application].
   */
  suspend fun getActuationPlan(application: String): ActuationPlan {
    val deliveryConfig = repository.getDeliveryConfigForApplication(application)
    return getActuationPlan(deliveryConfig)
  }

  /**
   * Returns the current point-in-time [ActuationPlan] for the specified [deliveryConfig].
   */
  suspend fun getActuationPlan(deliveryConfig: DeliveryConfig): ActuationPlan {
    val startTime = now
    val plan = ActuationPlan(
      application = deliveryConfig.application,
      timestamp = clock.instant(),
      environmentPlans = deliveryConfig.environments.map { env ->
        EnvironmentPlan(
          environment = env.name,
          resourcePlans = env.resources.mapNotNull { resource ->
            getResourceDiff(resource)?.let {
              ResourcePlan(
                resourceId = resource.id,
                diff = it.toConciseDeltaJson(),
                action = getActuationAction(resource, it)
              )
            }
          }
        )
      }
    )
    spectator.timer(
      APPLICATION_ACTUATION_PLAN_DURATION_ID,
      listOf(BasicTag("application", deliveryConfig.application))
    ).record(Duration.between(startTime, now))

    return plan
  }

  /**
   * @return The [ResourceDiff] for the specified [resource], by comparing its desired and current states.
   */
  private suspend fun <SPEC : ResourceSpec> getResourceDiff(resource: Resource<SPEC>): ResourceDiff<Any>? {
    val handler = handlers.supporting(resource.kind) as ResourceHandler<SPEC, *>
    return coroutineScope {
      val tasks = listOf(
        async {
          try {
            handler.desired(resource)
          } catch (e: Exception) {
            log.info("Failed to calculate the desired state", e)
          }
        },
        async {
          try {
            handler.current(resource)
          } catch (e: Exception) {
            log.info("failed to calculate the current state", e)
          }
        }
      )
      val (desired, current) = tasks.awaitAll()
      desired?.let {
        diffFactory.compare(it, current)
      }
    }
  }

  /**
   * @return What [ResourceAction] Keel would take based on the resource [diff].
   */
  private fun <SPEC : ResourceSpec> getActuationAction(resource: Resource<SPEC>, diff: ResourceDiff<Any>): ResourceAction {
    return when {
      !diff.hasChanges() -> NONE
      diff.current == null -> CREATE
      else -> UPDATE
    }
  }

  /**
   * When given a list of applications, check if the application is eligible for migration and if so,
   * send Slack notification to indicate the migration is ready
   */
  fun sendSlackNotificationForMigration(appNames: List<String>) {
    appNames.forEach { application ->
      val isMigratable = repository.getApplicationMigrationStatus(application).isMigratable
      if (isMigratable) {
        val submittedDeliveryConfig = repository.getMigratableApplicationData(application).autoGeneratedConfig
        publisher.publishEvent(MigrationReadyNotification(submittedDeliveryConfig.toDeliveryConfig()))
      }
    }
  }

  // delete a migration application in case of an error in the migration process
  fun deleteMigrationApplication(application: String) {
    val applicationPrData = repository.getMigratableApplicationData(application)
    runBlocking {
      try {
        //clean up the forked repository
        stashBridge.deleteFork(applicationPrData.repoSlug)
      } catch (ex: Exception) {
        log.error("caught an exception while deleting application $application fork named ${applicationPrData.repoSlug}. Will proceed with cleaning up.", ex)
      }
    }
    // delete the application from MD
    repository.deleteDeliveryConfigByApplication(application)
    repository.cleanPrLink(application)
    log.debug("application $application migration data was deleted successfully")
  }

  @ResponseStatus(HttpStatus.CONFLICT)
  private class ActionIncomplete :
    IllegalStateException("Verifications may only be retried once complete.")

  @ResponseStatus(HttpStatus.NOT_FOUND)
  private class InvalidActionId(id: String, context: ArtifactInEnvironmentContext) :
    IllegalStateException("Unknown verification id: $id. Expecting one of: ${context.verifications.map { it.id }}")
}

fun List<ConstraintState>.removePrivateConstraintAttrs() =
  map { state ->
    if (state.attributes?.type in ApplicationService.privateConstraintAttrs) {
      state.copy(attributes = null)
    } else {
      state
    }
  }

/**
 * Holds the info we need about artifacts in an environment for building the UI view.
 *
 * This is used in a list of versions pertaining to a specific delivery artifact.
 */
data class StatusInfoForArtifactInEnvironment(
  val version: String,
  val status: PromotionStatus,
  val replacedByVersion: String?,
  val deployedAt: Instant
)
