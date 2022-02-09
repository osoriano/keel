package com.netflix.spinnaker.keel.persistence

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.NotificationConfig
import com.netflix.spinnaker.keel.api.artifacts.ArtifactType
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.constraints.ConstraintState
import com.netflix.spinnaker.keel.api.migration.ApplicationMigrationStatus
import com.netflix.spinnaker.keel.migrations.ApplicationPrData
import com.netflix.spinnaker.keel.api.migration.SkippedPipeline
import com.netflix.spinnaker.keel.core.api.ApplicationSummary
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.core.api.UID
import com.netflix.spinnaker.keel.events.ResourceState
import com.netflix.spinnaker.keel.persistence.DependentAttachFilter.ATTACH_ALL
import com.netflix.spinnaker.kork.exceptions.ConfigurationException
import com.netflix.spinnaker.kork.exceptions.SystemException
import java.time.Duration
import java.time.Instant

interface DeliveryConfigRepository : PeriodicallyCheckedRepository<DeliveryConfig> {

  /**
   * Persists a [DeliveryConfig].
   */
  fun store(deliveryConfig: DeliveryConfig)

  /**
   * Retrieves a [DeliveryConfig] by its unique [name].
   *
   * @return The [DeliveryConfig]
   * @throws NoSuchDeliveryConfigException if [name] does not map to a persisted config
   */
  fun get(name: String): DeliveryConfig

  /**
   * @return The current count of unique delivery configs in the database.
   */
  fun count(): Int

  /**
   * @return The current count of unique environments in the database.
   */
  fun environmentCount(): Int

  /**
   * Retrieves all available [DeliveryConfig] entries in the database.
   *
   * Because this is a potentially expensive set of queries, this method allows you to specify
   * which "dependents" (artifacts, environments and preview environments) you want to load with
   * the delivery config. The default is to load the complete delivery config with all dependents
   * attached, but you can specify one or more filters depending on the data you're interested in.
   *
   * @return The set of all available [DeliveryConfig] entries.
   */
  fun all(vararg dependentAttachFilter: DependentAttachFilter = arrayOf(ATTACH_ALL)): Set<DeliveryConfig>

  /**
   * Retrieve the [Environment] a resource belongs to, by the resource [com.netflix.spinnaker.keel.api.Resource.id].
   */
  fun environmentFor(resourceId: String): Environment

  /**
   * Retrieve the [NotificationConfig] for an environment by environment name and delivery config name.
   */
  fun environmentNotifications(deliveryConfigName: String, environmentName: String): Set<NotificationConfig>

  /**
   * Retrieve the current status of all resources in [environmentName].
   */
  fun resourceStatusesInEnvironment(deliveryConfigName: String, environmentName: String) : Map<String, ResourceState>

  /**
   * Retrieve the [DeliveryConfig] a resource belongs to (the parent of its environment).
   */
  fun deliveryConfigFor(resourceId: String): DeliveryConfig

  /**
   * @return the [DeliveryConfig] associated with [application], throws [NoDeliveryConfigForApplication] if none
   */
  fun getByApplication(application: String): DeliveryConfig

  /**
   * @return the UID of the specified [deliveryConfig].
   * @throws NoSuchDeliveryConfigName if the delivery config is not found.
   */
  fun getUid(deliveryConfig: DeliveryConfig): String

  /**
   * @return true if [application] has a delivery config, otherwise false
   */
  fun isApplicationConfigured(application: String): Boolean

  /**
   * Deletes a delivery config and everything in it, based on the application name.
   */
  fun deleteByApplication(application: String)

  /**
   * Deletes a delivery config and everything in it.
   */
  fun deleteByName(name: String)

  /**
   * Removes a resource from an environment.
   */
  fun deleteResourceFromEnv(deliveryConfigName: String, environmentName: String, resourceId: String)

  /**
   * Deletes an environment from a delivery config.
   * Does not delete the resources within an environment
   */
  fun deleteEnvironment(deliveryConfigName: String, environmentName: String)


  /**
   * Deletes a preview environment spec associated with a delivery config.
   */
  fun deletePreviewEnvironment(deliveryConfigName: String, baseEnvironmentName: String)

  /**
   * Stores/updates an [Environment] associated with a [DeliveryConfig].
   *
   * Generally, updating environments should be done via [store]. This method is primarily
   * intended to support the creation of preview environments, where none of the other
   * properties of the delivery config have changed.
   */
  fun storeEnvironment(deliveryConfigName: String, environment: Environment)

  /**
   * Stores the association of the specified [artifact] with the [DeliveryConfig] with the given UID.
   *
   * Generally, storing this association should be done via [store]. This method is primarily
   * intended to support the creation of preview environments, where none of the other
   * properties of the delivery config have changed.
   */
  fun associateArtifact(deliveryConfigUid: String, artifact: DeliveryArtifact)

  /**
   * Updates state for a stateful [Environment] constraint.
   */
  fun storeConstraintState(state: ConstraintState)

  /**
   * Get the latest state of an [Environment] constraint for a specific artifact.
   *
   * @param deliveryConfigName the [DeliveryConfig] name
   * @param environmentName the [Environment] name
   * @param artifactVersion the version of the artifact we're checking constraint state for
   * @param type the type of constraint
   *
   * @return [ConstraintState] or `null` if the given constraint type has no state for
   * the given Artifact/Environment combination.
   */
  fun getConstraintState(
    deliveryConfigName: String,
    environmentName: String,
    artifactVersion: String,
    type: String,
    // FIXME: this should not be nullable as it's the only parameter uniquely identifying the artifact,
    //  but calls from the UI are currently not passing the artifact reference.
    artifactReference: String?
  ): ConstraintState?

  fun getConstraintStateById(
    uid: UID
  ): ConstraintState?

  /**
   * Removes constraint states from an [Environment] by [type].
   *
   * @param deliveryConfigName the [DeliveryConfig] name
   * @param environmentName the [Environment] name
   * @param type the type of the removed constraint
   */
  fun deleteConstraintState(
    deliveryConfigName: String,
    environmentName: String,
    reference: String,
    version: String,
    type: String
  ): Int

  /**
   * Rolls up the most recent constraint states (maximum of one per (Environment, ConstraintType))
   * related to a application retrieved by its name.
   *
   * @param application the application name
   *
   * @return A list of the most recent [ConstraintState]'s by environment per type or an
   * empty list if none exist.
   */
  fun constraintStateFor(application: String): List<ConstraintState>

  /**
   * Retrieves recent [ConstraintState]'s for an app, or for a list of given [Environment]s of that app.
   *
   * @param deliveryConfigName the [DeliveryConfig] name
   * @param environmentUIDs the [Environment] ids
   *
   * @return A list of [ConstraintState]'s or an empty list if none exist.
   */
  fun constraintStateForEnvironments(deliveryConfigName: String, environmentUIDs: List<String> = emptyList(), limit: Int? = null): List<ConstraintState>

  /**
   * Retrieves recent [ConstraintState]'s for an [Environment].
   *
   * @param deliveryConfigName the [DeliveryConfig] name
   * @param environmentName the [Environment] name
   * @param limit the maximum number of [ConstraintState]'s to return, sorted by recency
   *
   * @return A list of up-to the most recent `limit` [ConstraintState]'s or an empty list if
   * none exist.
   */
  fun constraintStateFor(
    deliveryConfigName: String,
    environmentName: String,
    limit: Int = 10
  ): List<ConstraintState>

  fun constraintStateFor(
    deliveryConfigName: String,
    environmentName: String,
    artifactVersion: String,
    artifactReference: String
  ): List<ConstraintState>

  /**
   * Fetches all versions have a pending stateful constraint in an environment
   */
  fun getPendingConstraintsForArtifactVersions(deliveryConfigName: String, environmentName: String, artifact: DeliveryArtifact): List<PublishedArtifact>

  /**
   * Gets all versions queued for approval for the environment
   */
  fun getArtifactVersionsQueuedForApproval(deliveryConfigName: String, environmentName: String, artifact: DeliveryArtifact): List<PublishedArtifact>

  /**
   * Adds an artifact version to the queued table to indicate all constraints pass for that version
   */
  fun queueArtifactVersionForApproval(deliveryConfigName: String, environmentName: String, artifact: DeliveryArtifact, artifactVersion: String)

  /**
   * Removes a queued version from the queued table
   */
  fun deleteArtifactVersionQueuedForApproval(deliveryConfigName: String, environmentName: String, artifact: DeliveryArtifact, artifactVersion: String)

  fun getApplicationSummaries(): Collection<ApplicationSummary>

  fun deliveryConfigLastChecked(deliveryConfig: DeliveryConfig): Instant

  /**
   * Resets the last checked time for the delivery config to the initial value
   * (EPOCH + 1s) so that the environments will immediately be rechecked.
   */
  fun triggerRecheck(application: String)

  /**
   * Associates [version] of [artifact] with the specified environment, creating a new environment
   * version if necessary.
   */
  fun addArtifactVersionToEnvironment(
    deliveryConfig: DeliveryConfig,
    environmentName: String,
    artifact: DeliveryArtifact,
    version: String,
  )

  /**
   * Returns the migration status of the app
   */
  fun getApplicationMigrationStatus(application: String): ApplicationMigrationStatus

  /**
   * Get a list of apps to run the export on. Excluding apps that are managed already
   */
  fun getAppsToExport(minTimeSinceLastCheck: Duration, batchSize: Int): List<String>

  /**
   * Store app in the migration DB if not exist.
   * [inAllowedList] defines if the app could be actively migrated if the export completed successfully
   */
  fun storeAppForPotentialMigration(app: String, inAllowedList: Boolean)


  /**
   * Return the information needed in order to open a PR for an application.
   * During a successful export process, the generated delivery config, repoSlug and projectKey and store them by app.
   */
  fun getMigratableApplicationData(app: String): ApplicationPrData

  /**
   * Storing the created pr link for an application
   */
  fun storePrLinkForMigratedApplication(app: String, prLink: String)

  /**
   * Storing the created jira link for an application
   */
  fun storeJiraLinkForMigratedApplication(app: String, jiraLink: String)

  /**
   * Check if a PR id is for migrating an app
   */
  fun isMigrationPr(application: String, prId: String): Boolean

  /**
   * Store the result of the pipelines export script
   */
  fun storePipelinesExportResult(
    deliveryConfig: SubmittedDeliveryConfig,
    skippedPipelines: List<SkippedPipeline>,
    exportSucceeded: Boolean,
    repoSlug: String? = null,
    projectKey: String? = null
  )

  /**
   * Store the result of a failed export in which we failed to generate any delivery config
   */
  fun storeFailedPipelinesExportResult(
    application: String,
    error: String,
  )

  /**
   * Mark the application migration status as blocked
   */
  fun markApplicationMigrationAsBlocked(application: String, reason: String, user: String): Boolean

  /**
   * Mark if the application is scm powered
   */
  fun updateMigratingAppScmStatus(application: String, isScmPowered: Boolean)
}

abstract class NoSuchDeliveryConfigException(message: String) :
  NoSuchEntityException(message)

class NoSuchDeliveryConfigName(name: String) :
  NoSuchDeliveryConfigException("No delivery config named $name exists in the database")

class NoDeliveryConfigForApplication(application: String) :
  NoSuchDeliveryConfigException("No delivery config for application $application exists in the database")

class ApplicationPullRequestDataIsMissing(application: String) :
  NoSuchEntityException("Either delivery config, repoSlug or projectKey is missing for $application in the database")

class NoMatchingArtifactException(deliveryConfigName: String, type: ArtifactType, reference: String) :
  NoSuchEntityException("No artifact with reference $reference and type $type found in delivery config $deliveryConfigName")

class TooManyDeliveryConfigsException(application: String, existing: String) :
  ConfigurationException("A delivery config already exists for application $application, and we only allow one per application - please delete existing config $existing before submitting a new config")

class ConflictingDeliveryConfigsException(application: String) :
  ConfigurationException("A delivery config already exists for a different application $application, and we don't allow delivery config name duplication - please select a different config name before submitting a new config")

class OrphanedResourceException(id: String) :
  SystemException("Resource $id exists without being a part of a delivery config")

class OverwritingExistingResourcesDisallowed(application: String, resource: String) :
  ConfigurationException("Cannot manage application $application. At least one resource already exists and would be overwritten: $resource")
