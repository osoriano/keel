package com.netflix.spinnaker.keel.persistence

import com.netflix.spinnaker.keel.api.ActionStateUpdateContext
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.NotificationConfig
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.ResourceDiffFactory
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.ResourceStatusSnapshot
import com.netflix.spinnaker.keel.api.action.Action
import com.netflix.spinnaker.keel.api.action.ActionRepository
import com.netflix.spinnaker.keel.api.action.ActionState
import com.netflix.spinnaker.keel.api.action.ActionStateFull
import com.netflix.spinnaker.keel.api.action.ActionType.VERIFICATION
import com.netflix.spinnaker.keel.api.artifacts.ArtifactMetadata
import com.netflix.spinnaker.keel.api.artifacts.ArtifactStatus
import com.netflix.spinnaker.keel.api.artifacts.ArtifactType
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.constraints.ConstraintState
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.api.events.ArtifactRegisteredEvent
import com.netflix.spinnaker.keel.api.events.ConstraintStateChanged
import com.netflix.spinnaker.keel.api.persistence.KeelReadOnlyRepository
import com.netflix.spinnaker.keel.core.api.ApplicationSummary
import com.netflix.spinnaker.keel.core.api.ArtifactSummaryInEnvironment
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactPin
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVeto
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVetoes
import com.netflix.spinnaker.keel.core.api.EnvironmentSummary
import com.netflix.spinnaker.keel.core.api.PinnedEnvironment
import com.netflix.spinnaker.keel.core.api.PromotionStatus
import com.netflix.spinnaker.keel.core.api.PublishedArtifactInEnvironment
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.core.api.UID
import com.netflix.spinnaker.keel.events.ApplicationEvent
import com.netflix.spinnaker.keel.events.ResourceCreated
import com.netflix.spinnaker.keel.events.ResourceEvent
import com.netflix.spinnaker.keel.events.ResourceHistoryEvent
import com.netflix.spinnaker.keel.events.ResourceState
import com.netflix.spinnaker.keel.events.ResourceUpdated
import com.netflix.spinnaker.keel.exceptions.DuplicateManagedResourceException
import com.netflix.spinnaker.keel.notifications.NotificationScope
import com.netflix.spinnaker.keel.notifications.NotificationType
import com.netflix.spinnaker.keel.services.StatusInfoForArtifactInEnvironment
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Propagation.REQUIRED
import org.springframework.transaction.annotation.Transactional
import java.time.Clock
import java.time.Duration
import java.time.Instant

/**
 * A combined repository for delivery configs, artifacts, and resources.
 *
 * This paves the way for re-thinking how we interact with sql/storing of resources,
 * and updating our internal repository structure to allow storing delivery configs to more easily also
 * store artifacts and resources.
 *
 * This also gives us an easy place to emit telemetry and events around the usage of methods.
 *
 * TODO eb: refactor repository interaction so transactionality is easier.
 */
@Component
class KeelRepository(
  private val deliveryConfigRepository: DeliveryConfigRepository,
  private val artifactRepository: ArtifactRepository,
  private val resourceRepository: ResourceRepository,
  private val actionRepository: ActionRepository,
  private val clock: Clock,
  private val publisher: ApplicationEventPublisher,
  private val diffFactory: ResourceDiffFactory,
  private val persistenceRetry: PersistenceRetry,
  private val notificationRepository: NotificationRepository
) : KeelReadOnlyRepository {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  // START Delivery config methods
  @Transactional(propagation = REQUIRED)
  fun upsertDeliveryConfig(submittedDeliveryConfig: SubmittedDeliveryConfig): DeliveryConfig {
    val new = submittedDeliveryConfig.toDeliveryConfig()
    return upsertDeliveryConfig(new)
  }

  @Transactional(propagation = REQUIRED)
  fun upsertDeliveryConfig(deliveryConfig: DeliveryConfig): DeliveryConfig {
    val configWithSameName = try {
      getDeliveryConfig(deliveryConfig.name)
    } catch (e: NoSuchDeliveryConfigException) {
      null
    }

    if (configWithSameName != null && configWithSameName.application != deliveryConfig.application) {
      // we don't allow storing 2 configs with the same name, for different applications
      throw ConflictingDeliveryConfigsException(configWithSameName.application)
    }

    val existingApplicationConfig = try {
      getDeliveryConfigForApplication(deliveryConfig.application)
    } catch (e: NoSuchDeliveryConfigException) {
      null
    }

    if (configWithSameName == null && existingApplicationConfig != null) {
      // we only allow one delivery config, so throw an error if someone is trying to submit a new config
      // instead of updating the existing config for the same application
      throw TooManyDeliveryConfigsException(deliveryConfig.application, existingApplicationConfig.name)
    }

    // by this point, configWithSameName is the old delivery config for this application
    deliveryConfig.resources.forEach { resource ->
      upsertResource(resource, deliveryConfig.name)
    }

    deliveryConfig.artifacts.forEach { artifact ->
      register(artifact)
    }

    storeDeliveryConfig(deliveryConfig)

    if (configWithSameName != null) {
      removeDependents(configWithSameName, deliveryConfig)
    }
    return getDeliveryConfig(deliveryConfig.name)
  }

  fun upsertPreviewEnvironment(
    deliveryConfig: DeliveryConfig,
    previewEnvironment: Environment,
    previewArtifacts: Set<DeliveryArtifact>
  ) {
    log.debug("Upserting preview environment $previewEnvironment for app '${deliveryConfig.application} (with retries)'")
    persistenceRetry.withRetry(RetryCategory.WRITE) {
      this.upsertPreviewEnvironmentWithoutRetries(deliveryConfig, previewEnvironment, previewArtifacts)
    }
  }

  @Transactional(propagation = REQUIRED)
  protected fun upsertPreviewEnvironmentWithoutRetries(
    deliveryConfig: DeliveryConfig,
    previewEnvironment: Environment,
    previewArtifacts: Set<DeliveryArtifact>
  ) {
    val deliveryConfigUid = deliveryConfigRepository.getUid(deliveryConfig)

    previewEnvironment.resources.forEach { resource ->
      upsertResource(resource, deliveryConfig.name)
    }

    previewArtifacts.forEach { artifact ->
      register(artifact)
      deliveryConfigRepository.associateArtifact(deliveryConfigUid, artifact)
    }

    deliveryConfigRepository.storeEnvironment(deliveryConfig.name, previewEnvironment)
  }

  /**
   * Removes artifacts, environments, and resources that were present in the [old]
   * delivery config and are not present in the [new] delivery config
   */
  fun removeDependents(old: DeliveryConfig, new: DeliveryConfig) {
    old.artifacts
      .filterNot { it.isPreview }
      .forEach { artifact ->
        val stillPresent = new.artifacts.any {
          it.name == artifact.name &&
            it.type == artifact.type &&
            it.reference == artifact.reference
        }
        if (!stillPresent) {
          log.debug("Updating config ${new.name}: removing artifact $artifact")
          artifactRepository.delete(artifact)
        }
      }

    val newResources = new.resources.map { it.id }

    old.environments
      .forEach { environment ->
        if (!environment.isPreview && environment.name !in new.environments.map { it.name }) {
          log.debug("Updating config ${new.name}: removing environment ${environment.name}")
          environment.resources.map(Resource<*>::id).forEach {
            // only delete the resource if it's not somewhere else in the delivery config -- e.g.
            // it's been moved from one environment to another or the environment has been renamed
            if (it !in newResources) {
              resourceRepository.delete(it)
            }
          }
          deliveryConfigRepository.deleteEnvironment(new.name, environment.name)
        }
      }

    old.previewEnvironments
      .forEach { previewEnvSpec ->
        if (previewEnvSpec.baseEnvironment !in new.previewEnvironments.map { it.baseEnvironment }) {
          log.debug("Updating config ${new.name}: removing preview environment spec $previewEnvSpec")
          deliveryConfigRepository.deletePreviewEnvironment(new.name, previewEnvSpec.baseEnvironment)
        }
      }

    /**
     * If a verification, V,  has been removed from the delivery config, and there's PENDING state
     * associated with V, then the EnvironmentExclusionEnforcer will block deployments and other verifications
     * from running.
     *
     * To avoid this problem, we update all PENDING records associated with delete verification V as OVERRIDE_FAIL
     */
    markRemovedPendingVerificationStateAsOverrideFail(old, new)
  }

  private fun markRemovedPendingVerificationStateAsOverrideFail(old: DeliveryConfig, new: DeliveryConfig) {
    val contexts = getVerificationRemovalContext(old, new)
    contexts.forEach {
      log.debug("Verification ${it.id} removed from ${old.application} ${it.environment.name}. Marking any PENDING state as OVERRIDE_FAIL.")
      actionRepository.updateState(it, ConstraintStatus.OVERRIDE_FAIL)
    }
  }
  /**
   * Generate a list of removal contexts that correspond to verifications that were removed from the delivery config
   */
  private fun getVerificationRemovalContext(old: DeliveryConfig, new: DeliveryConfig): List<ActionStateUpdateContext> {
    val deleted = old.verifications() subtract new.verifications()
    return deleted.mapNotNull { (e, id) ->
      new.environment(e)
        ?.let { env -> ActionStateUpdateContext(new, env, VERIFICATION, id) }
    }
  }

  /**
   * Return the set of verifications associated with a delivery config,
   * represented as pairs: (environment name, verification id)
   */
  fun DeliveryConfig.verifications() : Set<Pair<String, String>> =
    environments.flatMap { e ->
      e.verifyWith.map {v ->
        e.name to v.id
      }
    }.toSet()

  fun DeliveryConfig.environment(name : String) : Environment? =
    environments.firstOrNull { it.name == name }

  fun storeConstraintState(state: ConstraintState) {
    val previousState = getConstraintState(
      deliveryConfigName = state.deliveryConfigName,
      environmentName = state.environmentName,
      artifactVersion = state.artifactVersion,
      type = state.type,
      artifactReference = state.artifactReference
    )
    deliveryConfigRepository.storeConstraintState(state)
    if (!sameState(previousState, state)) {
      val config = getDeliveryConfig(state.deliveryConfigName)
      publisher.publishEvent(
        ConstraintStateChanged(
          environment = config.environments.first { it.name == state.environmentName },
          constraint = config.constraintInEnvironment(state.environmentName, state.type),
          deliveryConfig = config,
          previousState = previousState,
          currentState = state
        )
      )
    }
  }

  private fun sameState(previous: ConstraintState?, current: ConstraintState): Boolean =
    previous?.attributes == current.attributes
      && previous?.status == current.status
      && previous.judgedAt == current.judgedAt
      && previous.judgedBy == current.judgedBy
      && previous.comment == current.comment

  fun <T : ResourceSpec> upsertResource(resource: Resource<T>, deliveryConfigName: String) {
    val existingResource = try {
      getRawResource(resource.id)
    } catch (e: NoSuchResourceException) {
      null
    }
    if (existingResource != null) {
      // we allow resources to be managed in a single delivery config file
      val existingConfig = deliveryConfigFor(existingResource.id)
      if (existingConfig.name != deliveryConfigName) {
        log.debug("resource $resource is being managed already by delivery config named ${existingConfig.name}")
        throw DuplicateManagedResourceException(resource.id, existingConfig.name, deliveryConfigName)
      }

      val diff = diffFactory.compare(resource.spec, existingResource.spec)
      if (diff.hasChanges() || resource.kind.version != existingResource.kind.version) {
        log.debug("Updating ${resource.id}")
        storeResource(resource).also { updatedResource ->
          appendResourceHistory(ResourceUpdated(updatedResource, diff.toDeltaJson(), clock))
        }
      }
    } else {
      log.debug("Creating $resource")
      storeResource(resource).also { updatedResource ->
        appendResourceHistory(ResourceCreated(updatedResource, clock))
      }
    }
  }

  /**
   * Deletes a config and everything in it and about it
   */
  fun deleteDeliveryConfigByApplication(application: String) =
    deliveryConfigRepository.deleteByApplication(application)

  /**
   * Deletes a config and everything in it and about it
   */
  fun deleteDeliveryConfigByName(name: String) {
    deliveryConfigRepository.deleteByName(name)
  }

  fun storeDeliveryConfig(deliveryConfig: DeliveryConfig) =
    deliveryConfigRepository.store(deliveryConfig)

  fun getDeliveryConfigCount(): Int =
    deliveryConfigRepository.count()

  override fun getDeliveryConfig(name: String): DeliveryConfig =
    deliveryConfigRepository.get(name)

  fun getEnvironmentCount(): Int =
    deliveryConfigRepository.environmentCount()

  override fun environmentFor(resourceId: String): Environment =
    deliveryConfigRepository.environmentFor(resourceId)

  override fun environmentNotifications(deliveryConfigName: String, environmentName: String): Set<NotificationConfig> =
    deliveryConfigRepository.environmentNotifications(deliveryConfigName, environmentName)

  override fun deliveryConfigFor(resourceId: String): DeliveryConfig =
    deliveryConfigRepository.deliveryConfigFor(resourceId)

  override fun getDeliveryConfigForApplication(application: String): DeliveryConfig =
    deliveryConfigRepository.getByApplication(application)

  override fun isApplicationConfigured(application: String): Boolean =
    deliveryConfigRepository.isApplicationConfigured(application)

  fun isMigrationPr(application: String, prId: String): Boolean =
    deliveryConfigRepository.isMigrationPr(application, prId)

  override fun allDeliveryConfigs(vararg dependentAttachFilter: DependentAttachFilter): Set<DeliveryConfig> =
    deliveryConfigRepository.all(*dependentAttachFilter)

  fun deleteResourceFromEnv(deliveryConfigName: String, environmentName: String, resourceId: String) =
    deliveryConfigRepository.deleteResourceFromEnv(deliveryConfigName, environmentName, resourceId)

  fun deleteEnvironment(deliveryConfigName: String, environmentName: String) =
    deliveryConfigRepository.deleteEnvironment(deliveryConfigName, environmentName)

  override fun getConstraintState(deliveryConfigName: String, environmentName: String, artifactVersion: String, type: String, artifactReference: String?): ConstraintState? =
    deliveryConfigRepository.getConstraintState(deliveryConfigName, environmentName, artifactVersion, type, artifactReference)

  override fun constraintStateFor(deliveryConfigName: String, environmentName: String, artifactVersion: String, artifactReference: String): List<ConstraintState> =
    deliveryConfigRepository.constraintStateFor(deliveryConfigName, environmentName, artifactVersion, artifactReference)

  override fun getPendingConstraintsForArtifactVersions(deliveryConfigName: String, environmentName: String, artifact: DeliveryArtifact): List<PublishedArtifact> =
    deliveryConfigRepository.getPendingConstraintsForArtifactVersions(deliveryConfigName, environmentName, artifact)

  override fun getArtifactVersionsQueuedForApproval(deliveryConfigName: String, environmentName: String, artifact: DeliveryArtifact): List<PublishedArtifact> =
    deliveryConfigRepository.getArtifactVersionsQueuedForApproval(deliveryConfigName, environmentName, artifact)

  fun queueArtifactVersionForApproval(deliveryConfigName: String, environmentName: String, artifact: DeliveryArtifact, artifactVersion: String) =
    deliveryConfigRepository.queueArtifactVersionForApproval(deliveryConfigName, environmentName, artifact, artifactVersion)

  fun deleteArtifactVersionQueuedForApproval(deliveryConfigName: String, environmentName: String, artifact: DeliveryArtifact, artifactVersion: String) =
    deliveryConfigRepository.deleteArtifactVersionQueuedForApproval(deliveryConfigName, environmentName, artifact, artifactVersion)

  fun getConstraintStateById(uid: UID): ConstraintState? =
    deliveryConfigRepository.getConstraintStateById(uid)

  fun deleteConstraintState(deliveryConfigName: String, environmentName: String, reference: String, version: String, type: String): Int =
    deliveryConfigRepository.deleteConstraintState(deliveryConfigName, environmentName, reference, version, type)

  override fun constraintStateFor(application: String): List<ConstraintState> =
    deliveryConfigRepository.constraintStateFor(application)

  override fun constraintStateFor(
    deliveryConfigName: String,
    environmentName: String,
    limit: Int
  ): List<ConstraintState> =
    deliveryConfigRepository.constraintStateFor(deliveryConfigName, environmentName, limit)

  override fun constraintStateForEnvironments(
    deliveryConfigName: String,
    environmentUIDs: List<String>
  ): List<ConstraintState> =
    deliveryConfigRepository.constraintStateForEnvironments(deliveryConfigName, environmentUIDs)

  fun deliveryConfigsDueForCheck(minTimeSinceLastCheck: Duration, limit: Int): Collection<DeliveryConfig> =
    deliveryConfigRepository.itemsDueForCheck(minTimeSinceLastCheck, limit)

  fun markResourceCheckComplete(resource: Resource<*>, state: ResourceState) {
    resourceRepository.markCheckComplete(resource, state)
  }

  fun markDeliveryConfigCheckComplete(deliveryConfig: DeliveryConfig) {
    deliveryConfigRepository.markCheckComplete(deliveryConfig, null)
  }

  fun getApplicationSummaries(): Collection<ApplicationSummary> =
    deliveryConfigRepository.getApplicationSummaries()

  fun triggerDeliveryConfigRecheck(application: String) =
    deliveryConfigRepository.triggerRecheck(application)
  // END DeliveryConfigRepository methods

  // START ResourceRepository methods
  fun allResources(callback: (ResourceHeader) -> Unit) =
    resourceRepository.allResources(callback)

  fun getResourceCount() =
    resourceRepository.count()

  override fun getResource(id: String): Resource<ResourceSpec> =
    resourceRepository.get(id)

  fun getResourceStatus(id: String): ResourceStatusSnapshot? =
    resourceRepository.getStatus(id)

  override fun getRawResource(id: String): Resource<ResourceSpec> =
    resourceRepository.getRaw(id)

  override fun hasManagedResources(application: String): Boolean =
    resourceRepository.hasManagedResources(application)

  override fun getResourceIdsByApplication(application: String): List<String> =
    resourceRepository.getResourceIdsByApplication(application)

  override fun getResourcesByApplication(application: String): List<Resource<*>> =
    resourceRepository.getResourcesByApplication(application)

  fun <T : ResourceSpec> storeResource(resource: Resource<T>): Resource<T> =
    resourceRepository.store(resource)

  fun deleteResource(id: String) =
    resourceRepository.delete(id)

  fun applicationEventHistory(application: String, limit: Int): List<ApplicationEvent> =
    resourceRepository.applicationEventHistory(application, limit)

  fun applicationEventHistory(application: String, downTo: Instant): List<ApplicationEvent> =
    resourceRepository.applicationEventHistory(application, downTo)

  fun resourceEventHistory(id: String, limit: Int): List<ResourceHistoryEvent> =
    resourceRepository.eventHistory(id, limit)

  fun lastResourceHistoryEvent(id: String): ResourceHistoryEvent? =
    resourceRepository.lastEvent(id)

  fun appendResourceHistory(event: ResourceEvent) =
    resourceRepository.appendHistory(event)

  fun appendApplicationHistory(event: ApplicationEvent) =
    resourceRepository.appendHistory(event)

  fun resourcesDueForCheck(minTimeSinceLastCheck: Duration, limit: Int): Collection<Resource<ResourceSpec>> =
    resourceRepository.itemsDueForCheck(minTimeSinceLastCheck, limit)

  fun triggerResourceRecheck(environmentName: String, application: String) =
    resourceRepository.triggerResourceRecheck(environmentName, application)
  // END ResourceRepository methods

  // START ArtifactRepository methods
  fun register(artifact: DeliveryArtifact) {
    if (artifactRepository.register(artifact)) {
      publisher.publishEvent(ArtifactRegisteredEvent(artifact))
    }
  }

  fun artifactsDueForCheck(minTimeSinceLastCheck: Duration, limit: Int): Collection<DeliveryArtifact> =
    artifactRepository.itemsDueForCheck(minTimeSinceLastCheck, limit)

  override fun getArtifact(name: String, type: ArtifactType, deliveryConfigName: String): List<DeliveryArtifact> =
    artifactRepository.get(name, type, deliveryConfigName)

  override fun getArtifact(name: String, type: ArtifactType, reference: String, deliveryConfigName: String): DeliveryArtifact =
    artifactRepository.get(name, type, reference, deliveryConfigName)

  override fun getArtifact(deliveryConfigName: String, reference: String): DeliveryArtifact =
    artifactRepository.get(deliveryConfigName, reference)

  override fun isRegistered(name: String, type: ArtifactType): Boolean =
    artifactRepository.isRegistered(name, type)

  fun getAllArtifacts(type: ArtifactType? = null, name: String? = null): List<DeliveryArtifact> =
    artifactRepository.getAll(type, name)

  fun storeArtifactVersion(artifactVersion: PublishedArtifact): Boolean =
    artifactRepository.storeArtifactVersion(artifactVersion)

  override fun getArtifactVersion(artifact: DeliveryArtifact, version: String, status: ArtifactStatus?): PublishedArtifact? =
    artifactRepository.getArtifactVersion(artifact, version, status)

  fun getLatestApprovedInEnvArtifactVersion(config: DeliveryConfig, artifact: DeliveryArtifact, environmentName: String, excludeCurrent: Boolean?): PublishedArtifact? =
    artifactRepository.getApprovedInEnvArtifactVersion(config, artifact, environmentName, excludeCurrent)

  fun updateArtifactMetadata(artifact: PublishedArtifact, artifactMetadata: ArtifactMetadata) =
    artifactRepository.updateArtifactMetadata(artifact, artifactMetadata)

  fun deleteArtifact(artifact: DeliveryArtifact) =
    artifactRepository.delete(artifact)

  override fun artifactVersions(artifact: DeliveryArtifact, limit: Int): List<PublishedArtifact> =
    artifactRepository.versions(artifact, limit)

  override fun getVersionsWithoutMetadata(limit: Int, maxAge: Duration): List<PublishedArtifact> =
    artifactRepository.getVersionsWithMissingMetadata(limit, maxAge)

  override fun latestVersionApprovedIn(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, targetEnvironment: String): String? =
    artifactRepository.latestVersionApprovedIn(deliveryConfig, artifact, targetEnvironment)

  fun approveVersionFor(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, version: String, targetEnvironment: String): Boolean =
    artifactRepository.approveVersionFor(deliveryConfig, artifact, version, targetEnvironment)

  fun getApprovedAt(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, version: String, targetEnvironment: String): Instant? =
    artifactRepository.getApprovedAt(deliveryConfig, artifact, version, targetEnvironment)

  fun getPinnedAt(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, version: String, targetEnvironment: String): Instant? =
    artifactRepository.getPinnedAt(deliveryConfig, artifact, version, targetEnvironment)

  override fun isApprovedFor(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, version: String, targetEnvironment: String): Boolean =
    artifactRepository.isApprovedFor(deliveryConfig, artifact, version, targetEnvironment)

  fun markAsDeployingTo(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, version: String, targetEnvironment: String) =
    artifactRepository.markAsDeployingTo(deliveryConfig, artifact, version, targetEnvironment)

  override fun wasSuccessfullyDeployedTo(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, version: String, targetEnvironment: String): Boolean =
    artifactRepository.wasSuccessfullyDeployedTo(deliveryConfig, artifact, version, targetEnvironment)

  override fun isCurrentlyDeployedTo(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, version: String, targetEnvironment: String): Boolean =
    artifactRepository.isCurrentlyDeployedTo(deliveryConfig, artifact, version, targetEnvironment)

  override fun getCurrentlyDeployedArtifactVersion(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    environmentName: String
  ): PublishedArtifact? =
    artifactRepository.getCurrentlyDeployedArtifactVersion(deliveryConfig, artifact, environmentName)

  override fun getReleaseStatus(artifact: DeliveryArtifact, version: String): ArtifactStatus? =
    artifactRepository.getReleaseStatus(artifact, version)

  fun markAsSuccessfullyDeployedTo(
    deliveryConfig: DeliveryConfig,
    artifact: DeliveryArtifact,
    version: String,
    targetEnvironment: String
  ) =
    artifactRepository.markAsSuccessfullyDeployedTo(deliveryConfig, artifact, version, targetEnvironment)

  fun getArtifactVersionsByStatus(
    deliveryConfig: DeliveryConfig,
    environmentName: String,
    artifactReference: String,
    statuses: List<PromotionStatus>
  ): List<PublishedArtifact> =
    artifactRepository.getArtifactVersionsByStatus(deliveryConfig, artifactReference, environmentName, statuses)

  fun getArtifactPromotionStatus(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, version: String, targetEnvironment: String): PromotionStatus? =
    artifactRepository.getArtifactPromotionStatus(deliveryConfig, artifact, version, targetEnvironment)

  fun getPendingVersionsInEnvironment(
    deliveryConfig: DeliveryConfig,
    artifactReference: String,
    environmentName: String
  ): List<PublishedArtifact> =
    artifactRepository.getPendingVersionsInEnvironment(deliveryConfig, artifactReference, environmentName)

  fun getNotYetDeployedVersionsInEnvironment(
    deliveryConfig: DeliveryConfig,
    artifactReference: String,
    environmentName: String
  ): List<PublishedArtifact> =
    artifactRepository.getNotYetDeployedVersionsInEnvironment(deliveryConfig, artifactReference, environmentName)

  fun getNumPendingToBePromoted(
    application: String,
    artifactReference: String,
    environmentName: String,
    version: String
  ): Int =
    artifactRepository.getNumPendingToBePromoted(
      getDeliveryConfigForApplication(application),
      artifactReference,
      environmentName,
      version
    )

  fun getAllVersionsForEnvironment(
    artifact: DeliveryArtifact, config: DeliveryConfig, environmentName: String
  ): List<PublishedArtifactInEnvironment> =
    artifactRepository.getAllVersionsForEnvironment(artifact, config, environmentName)

  fun getEnvironmentSummaries(deliveryConfig: DeliveryConfig): List<EnvironmentSummary> =
    artifactRepository.getEnvironmentSummaries(deliveryConfig)

  fun pinEnvironment(deliveryConfig: DeliveryConfig, environmentArtifactPin: EnvironmentArtifactPin) =
    artifactRepository.pinEnvironment(deliveryConfig, environmentArtifactPin)

  fun pinnedEnvironments(deliveryConfig: DeliveryConfig): List<PinnedEnvironment> =
    artifactRepository.getPinnedEnvironments(deliveryConfig)

  fun deletePin(deliveryConfig: DeliveryConfig, targetEnvironment: String, reference: String? = null) =
    if (reference != null) {
      artifactRepository.deletePin(deliveryConfig, targetEnvironment, reference)
    } else {
      artifactRepository.deletePin(deliveryConfig, targetEnvironment)
    }

  fun vetoedEnvironmentVersions(deliveryConfig: DeliveryConfig): List<EnvironmentArtifactVetoes> =
    artifactRepository.vetoedEnvironmentVersions(deliveryConfig)

  fun markAsVetoedIn(
    deliveryConfig: DeliveryConfig,
    veto: EnvironmentArtifactVeto,
    force: Boolean
  ): Boolean =
    artifactRepository.markAsVetoedIn(deliveryConfig, veto, force)

  fun deleteVeto(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, version: String, targetEnvironment: String) =
    artifactRepository.deleteVeto(deliveryConfig, artifact, version, targetEnvironment)

  fun markAsSkipped(deliveryConfig: DeliveryConfig, artifact: DeliveryArtifact, version: String, targetEnvironment: String, supersededByVersion: String?) {
    artifactRepository.markAsSkipped(deliveryConfig, artifact, version, targetEnvironment, supersededByVersion)
  }

  /**
   * Given information about a delivery config, environment, artifact and a list of versions
   * returns a list of summaries for all versions that can be
   * used by the UI.
   */
  fun getArtifactSummariesInEnvironment(
    deliveryConfig: DeliveryConfig,
    environmentName: String,
    artifactReference: String,
    versions: List<String>
  ): List<ArtifactSummaryInEnvironment> =
    artifactRepository.getArtifactSummariesInEnvironment(
      deliveryConfig, environmentName, artifactReference, versions
    )

  /**
   * Given information about a delivery config, environment, artifact and version, returns a summary that can be
   * used by the UI, or null if the artifact version is not applicable to the environment.
   */
  @Deprecated("Replace with the bulk call `getArtifactSummariesInEnvironment(...)` above")
  fun getArtifactSummaryInEnvironment(
    deliveryConfig: DeliveryConfig,
    environmentName: String,
    artifactReference: String,
    version: String
  ) = artifactRepository.getArtifactSummaryInEnvironment(
    deliveryConfig, environmentName, artifactReference, version
  )

  /**
   * Return the published artifact for the last deployed version that matches the promotion status
   */
  fun getArtifactVersionByPromotionStatus(
    deliveryConfig: DeliveryConfig,
    environmentName: String,
    artifact: DeliveryArtifact,
    promotionStatus: PromotionStatus,
    version: String? = null
  ) = artifactRepository.getArtifactVersionByPromotionStatus(
    deliveryConfig, environmentName, artifact, promotionStatus, version
  )

  fun getVersionInfoInEnvironment(
    deliveryConfig: DeliveryConfig,
    environmentName: String,
    artifact: DeliveryArtifact
  ): List<StatusInfoForArtifactInEnvironment> =
    artifactRepository.getVersionInfoInEnvironment(deliveryConfig, environmentName, artifact)

  /**
   * Return a specific artifact version if is pinned, from [targetEnvironment], by [reference], if exists.
   */
  fun getPinnedVersion(deliveryConfig: DeliveryConfig, targetEnvironment: String, reference: String) =
    artifactRepository.getPinnedVersion(deliveryConfig, targetEnvironment, reference)

  // END ArtifactRepository methods

  // START ActionRepository methods
  fun nextEnvironmentsForVerification(
    minTimeSinceLastCheck: Duration,
    limit: Int
  ) : Collection<ArtifactInEnvironmentContext> =
    actionRepository.nextEnvironmentsForVerification(minTimeSinceLastCheck, limit)

  fun nextEnvironmentsForPostDeployAction(
    minTimeSinceLastCheck: Duration,
    limit: Int
  ): Collection<ArtifactInEnvironmentContext> =
    actionRepository.nextEnvironmentsForPostDeployAction(minTimeSinceLastCheck, limit)

  override fun getVerificationStatesBatch(contexts: List<ArtifactInEnvironmentContext>) : List<Map<String, ActionState>> =
    actionRepository.getStatesBatch(contexts, VERIFICATION)

  override fun getActionState(context: ArtifactInEnvironmentContext, action: Action): ActionState? {
    return actionRepository.getState(context, action)
  }

  /**
   * Updates the state of [action] as run against [context].
   *
   * @param metadata if non-empty this will overwrite any existing metadata.
   */
  fun updateActionState(
    context: ArtifactInEnvironmentContext,
    action: Action,
    status: ConstraintStatus,
    metadata: Map<String, Any?> = emptyMap(),
    link: String? = null
  ) = actionRepository.updateState(context, action, status, metadata, link)

  /**
   * Resets the state of [action] run against [context]
   */
  fun resetActionState(
    context: ArtifactInEnvironmentContext,
    action:
    Action,
    user: String
  ): ConstraintStatus = actionRepository.resetState(context, action, user)

  override fun getAllActionStatesBatch(contexts: List<ArtifactInEnvironmentContext>): List<List<ActionStateFull>> =
    actionRepository.getAllStatesBatch(contexts)

  // END ActionRepository methods

  /**
   * Store app in the migration DB if not exist.
   * [inAllowedList] defines if the app could be actively migrated if the export completed successfully
   */
  fun storeAppForPotentialMigration(app: String, inAllowList: Boolean?) =
    deliveryConfigRepository.storeAppForPotentialMigration(app, inAllowList)

  /**
   * Resets the [LAST_CHECKED] flag of an app to get it rechecked on the next cycle
   */
  fun triggerMigratingAppsRecheck(applications: List<String>) =
    deliveryConfigRepository.triggerMigratingAppsRecheck(applications)

  /**
   * Return the information needed in order to open a PR for an application.
   * During a successful export process, the generated delivery config, repoSlug and projectKey and store them by app.
   */
  fun getMigratableApplicationData(app: String) =
    deliveryConfigRepository.getMigratableApplicationData(app)

  /**
   * Storing the created pr link for an application
   */
  fun storePrLinkForMigratedApplication(application: String, prLink: String) =
    deliveryConfigRepository.storePrLinkForMigratedApplication(application, prLink)

  /**
   * Storing the jira issue link for an application
   */
  fun storeJiraLinkForMigratedApplication(application: String, jiraLink: String) =
    deliveryConfigRepository.storeJiraLinkForMigratedApplication(application, jiraLink)

  /**
   * Return migration data fro an application
   */
  fun getApplicationMigrationStatus(application: String) =
    deliveryConfigRepository.getApplicationMigrationStatus(application)

  override fun versionsInUse(artifact: DeliveryArtifact): Set<String> =
    artifactRepository.versionsInUse(artifact)

  fun markSentSlackNotification(type: NotificationType, application: String) =
    notificationRepository.addNotification(NotificationScope.APPLICATION, application, type)

}
