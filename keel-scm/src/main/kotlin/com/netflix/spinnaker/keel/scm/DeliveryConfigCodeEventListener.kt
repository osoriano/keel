package com.netflix.spinnaker.keel.scm

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.Repo
import com.netflix.spinnaker.keel.auth.AuthorizationResourceType.SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.auth.AuthorizationSupport
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.model.Application
import com.netflix.spinnaker.keel.front50.model.GitRepository
import com.netflix.spinnaker.keel.front50.model.ManagedDeliveryConfig
import com.netflix.spinnaker.keel.igor.DeliveryConfigImporter
import com.netflix.spinnaker.keel.notifications.DeliveryConfigImportFailed
import com.netflix.spinnaker.keel.persistence.DismissibleNotificationRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.PausedRepository
import com.netflix.spinnaker.keel.retrofit.isNotFound
import com.netflix.spinnaker.keel.telemetry.safeIncrement
import com.netflix.spinnaker.keel.upsert.DeliveryConfigUpserter
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.core.env.Environment
import org.springframework.security.access.AccessDeniedException
import org.springframework.stereotype.Component
import java.time.Clock

/**
 * Listens to commit events from applications' default source code branch to import their delivery configs from source.
 */
@Component
class DeliveryConfigCodeEventListener(
  private val keelRepository: KeelRepository,
  private val deliveryConfigUpserter: DeliveryConfigUpserter,
  private val deliveryConfigImporter: DeliveryConfigImporter,
  private val notificationRepository: DismissibleNotificationRepository,
  private val front50Cache: Front50Cache,
  private val scmUtils: ScmUtils,
  private val springEnv: Environment,
  private val spectator: Registry,
  private val eventPublisher: ApplicationEventPublisher,
  private val authorizationSupport: AuthorizationSupport,
  private val clock: Clock,
  private val pausedRepository: PausedRepository,
) {
  companion object {
    private val log by lazy { LoggerFactory.getLogger(DeliveryConfigCodeEventListener::class.java) }
    internal const val CODE_EVENT_COUNTER = "importConfig.codeEvent.count"
  }

  private val enabled: Boolean
    get() = springEnv.getProperty("keel.importDeliveryConfigs.enabled", Boolean::class.java, true)

  /**
   * Listens to [CommitCreatedEvent] events to catch those that match the default branch
   * associated with any applications configured in Spinnaker.
   *
   * When a match is found, retrieves the delivery config from the target branch, and stores
   * it in the database. This is our main path for supporting SCM integration to monitor
   * delivery config changes in source repos.
   */
  @EventListener(CommitCreatedEvent::class, PrMergedEvent::class)
  fun handleCodeEvent(event: CodeEvent) {
    if (!enabled) {
      log.debug("Importing delivery config from source disabled by feature flag. Ignoring commit event: $event")
      return
    }

    val apps = runBlocking {
      try {
        front50Cache.searchApplicationsByRepo(GitRepository(event.repoType, event.targetProjectKey, event.targetRepoSlug))
          .also {
            log.debug("Retrieved ${it.size} applications from Front50")
          }
      } catch (e: Exception) {
        log.error("Error searching applications: $e", e)
        null
      }
    } ?: return

    // Hopefully this returns a single matching app, but who knows... ¯\_(ツ)_/¯
    val matchingApps = apps
      .filter { app ->
        event.matchesApplicationConfig(app)
          && event.targetBranch == scmUtils.getDefaultBranch(app)
          && (isAutoImportEnabled(app) || isMigrationPr(event, app))
      }

    if (matchingApps.isEmpty()) {
      log.debug("No applications with matching SCM config found for event: $event")
      return
    }

    log.debug("Processing code event: $event")
    matchingApps.forEach { app ->
      log.debug("Importing delivery config for app ${app.name} from branch ${event.targetBranch}, commit ${event.commitHash}")

      // We always want to dismiss the previous notifications, and if needed to create a new one
      notificationRepository.dismissNotification(DeliveryConfigImportFailed::class.java, app.name, event.targetBranch)

      try {
        val user = event.causeByEmail ?: event.authorEmail ?: error("Can't authorize import due to missing author e-mail in code event: $event")
        val deliveryConfig = deliveryConfigImporter.import(
          codeEvent = event,
          manifestPath = app.managedDelivery?.manifestPath
        ).let {
          if (it.serviceAccount == null) {
            it.copy(serviceAccount = app.email)
          } else {
            it
          }
        }.also {
          authorizeServiceAccountAccess(
            user = user,
            deliveryConfig = it
          )
        }
        val gitMetadata = event.commitHash?.let {
          GitMetadata(
            commit = it,
            author = event.authorName,
            project = event.projectKey,
            branch = event.targetBranch,
            repo = Repo(name = event.repoKey),
            commitInfo = Commit(sha = event.commitHash, link = scmUtils.getCommitLink(event), message = event.message)
          )
        }
        log.debug("Creating/updating delivery config for application ${app.name} from branch ${event.targetBranch}")
        val isNew = deliveryConfigUpserter.upsertConfig(deliveryConfig, gitMetadata).second
        log.debug("Delivery config for application ${app.name} updated successfully from branch ${event.targetBranch}")
        event.emitCounterMetric(CODE_EVENT_COUNTER, DELIVERY_CONFIG_RETRIEVAL_SUCCESS, app.name)
        if (isNew) {
          onboardNewApplication(app, user, event)
        }
      } catch (e: Exception) {
        log.error("Error retrieving/updating delivery config for application ${app.name}: $e", e)
        event.emitCounterMetric(CODE_EVENT_COUNTER, DELIVERY_CONFIG_RETRIEVAL_ERROR, app.name)
        when {
          e.isNotFound -> log.debug("Skipping publishing event for delivery config not found: $e")
          e is AccessDeniedException -> log.debug("Skipping publishing event for access denied importing config: $e")
          else -> {
            eventPublisher.publishDeliveryConfigImportFailed(
              app.name,
              event,
              event.targetBranch,
              clock.instant(),
              e.message ?: "Unknown reason",
              scmUtils.getCommitLink(event)
            )
          }
        }
        return@forEach
      }
    }
  }

  private fun isMigrationPr(
      event: CodeEvent,
      app: Application
  ) = (event.pullRequestId?.let { keelRepository.isMigrationPr(app.name, it) }) ?: false

  private fun isAutoImportEnabled(app: Application) =
    app.managedDelivery?.importDeliveryConfig == true && keelRepository.isApplicationConfigured(app.name)


  /**
   * Checks that the owner of the app has access to the service account configured in the delivery config.
   */
  private fun authorizeServiceAccountAccess(user: String, deliveryConfig: SubmittedDeliveryConfig) {
    authorizationSupport.checkPermission(
      user = user,
      resourceName = deliveryConfig.serviceAccount
        ?: error("Delivery config for application ${deliveryConfig.application} is missing required service account."),
      resourceType = SERVICE_ACCOUNT,
      permission = "ACCESS"
    )
  }

  private fun onboardNewApplication(application: Application, user: String, event: CodeEvent) {
    runBlocking {
      front50Cache.updateManagedDeliveryConfig(application, user, ManagedDeliveryConfig(importDeliveryConfig = true))
      if (isMigrationPr(event, application)) {
        front50Cache.disableAllPipelines(application.name)
        pausedRepository.resumeApplication(application.name)
      }
    }
  }

  private fun CodeEvent.emitCounterMetric(
    metric: String,
    extraTags: Collection<Pair<String, String>>,
    application: String? = null
  ) =
    spectator.counter(metric, metricTags(application, extraTags)).safeIncrement()
}
