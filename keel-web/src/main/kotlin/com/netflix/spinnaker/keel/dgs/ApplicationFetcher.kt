package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsData
import com.netflix.graphql.dgs.DgsDataFetchingEnvironment
import com.netflix.graphql.dgs.InputArgument
import com.netflix.graphql.dgs.context.DgsContext
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.action.ActionType
import com.netflix.spinnaker.keel.artifacts.ArtifactVersionLinks
import com.netflix.spinnaker.keel.auth.AuthorizationSupport
import com.netflix.spinnaker.keel.core.api.DependsOnConstraint
import com.netflix.spinnaker.keel.events.EventLevel.ERROR
import com.netflix.spinnaker.keel.events.EventLevel.WARNING
import com.netflix.spinnaker.keel.graphql.DgsConstants
import com.netflix.spinnaker.keel.graphql.types.MD_Action
import com.netflix.spinnaker.keel.graphql.types.MD_Application
import com.netflix.spinnaker.keel.graphql.types.MD_ApplicationResult
import com.netflix.spinnaker.keel.graphql.types.MD_Artifact
import com.netflix.spinnaker.keel.graphql.types.MD_ArtifactStatusInEnvironment
import com.netflix.spinnaker.keel.graphql.types.MD_ArtifactVersionInEnvironment
import com.netflix.spinnaker.keel.graphql.types.MD_ComparisonLinks
import com.netflix.spinnaker.keel.graphql.types.MD_Constraint
import com.netflix.spinnaker.keel.graphql.types.MD_Environment
import com.netflix.spinnaker.keel.graphql.types.MD_EnvironmentState
import com.netflix.spinnaker.keel.graphql.types.MD_Error
import com.netflix.spinnaker.keel.graphql.types.MD_GitMetadata
import com.netflix.spinnaker.keel.graphql.types.MD_LifecycleStep
import com.netflix.spinnaker.keel.graphql.types.MD_Notification
import com.netflix.spinnaker.keel.graphql.types.MD_PackageDiff
import com.netflix.spinnaker.keel.graphql.types.MD_PausedInfo
import com.netflix.spinnaker.keel.graphql.types.MD_PinnedVersion
import com.netflix.spinnaker.keel.graphql.types.MD_PullRequest
import com.netflix.spinnaker.keel.graphql.types.MD_VersionVeto
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.persistence.DismissibleNotificationRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.persistence.NoDeliveryConfigForApplication
import com.netflix.spinnaker.keel.scm.ScmUtils
import graphql.execution.DataFetcherResult
import graphql.schema.DataFetchingEnvironment
import org.dataloader.DataLoader
import org.springframework.security.access.prepost.PreAuthorize
import java.util.concurrent.CompletableFuture

/**
 * Fetches details about an application, as defined in [schema.graphql]
 *
 * Loads the static data from a delivery config.
 * Adds supplemental data using functions to fetch more information.
 */
@DgsComponent
class ApplicationFetcher(
  private val authorizationSupport: AuthorizationSupport,
  private val keelRepository: KeelRepository,
  private val actuationPauser: ActuationPauser,
  private val artifactVersionLinks: ArtifactVersionLinks,
  private val applicationFetcherSupport: ApplicationFetcherSupport,
  private val notificationRepository: DismissibleNotificationRepository,
  private val scmUtils: ScmUtils,
) {

  @DgsData(parentType = DgsConstants.QUERY.TYPE_NAME, field = DgsConstants.QUERY.Md_application)
  @PreAuthorize("""@authorizationSupport.hasApplicationPermission('READ', 'APPLICATION', #appName)""")
  fun application(dfe: DataFetchingEnvironment, @InputArgument("appName") appName: String): MD_ApplicationResult {
    val config = try {
      keelRepository.getDeliveryConfigForApplication(appName)
    } catch (ex: NoDeliveryConfigForApplication) {
      return MD_Error(id = appName, message = "Delivery config not found")
    }
    val context: ApplicationContext = DgsContext.getCustomContext(dfe)
    context.deliveryConfig = config
    return MD_Application(
      id = config.application,
      name = config.application,
      account = config.serviceAccount,
      environments = emptyList()
    )
  }

  @DgsData(parentType = DgsConstants.MD_APPLICATION.TYPE_NAME, field = DgsConstants.MD_APPLICATION.Environments)
  fun environments(dfe: DgsDataFetchingEnvironment): List<DataFetcherResult<MD_Environment>> {
    val config = applicationFetcherSupport.getDeliveryConfigFromContext(dfe)
    return config.environments.sortedWith { env1, env2 ->
      when {
        env1.dependsOn(env2) -> -1
        env2.dependsOn(env1) -> 1
        env1.hasDependencies() && !env2.hasDependencies() -> -1
        env2.hasDependencies() && !env1.hasDependencies() -> 1
        else -> 0
      }
    }.map { env ->
      val artifacts = config.artifactsUsedIn(env.name).map { artifact ->
        MD_Artifact(
          id = "${env.name}-${artifact.reference}",
          environment = env.name,
          name = artifact.name,
          reference = artifact.reference,
          type = artifact.type
        )
      }
      DataFetcherResult.newResult<MD_Environment>().data(
        MD_Environment(
          id = env.name,
          name = env.name,
          isPreview = env.isPreview,
          basedOn = env.basedOn,
          state = MD_EnvironmentState(
            id = "${env.name}-state",
            artifacts = artifacts,
            resources = env.resources.map { it.toDgs(config, env.name) }
          ),
        )
      ).localContext(env).build()
    }
  }

  @DgsData(parentType = DgsConstants.MD_ENVIRONMENT.TYPE_NAME, field = DgsConstants.MD_ENVIRONMENT.GitMetadata)
  fun environmentGitMetadata(dfe: DgsDataFetchingEnvironment): MD_GitMetadata? {
    val env: Environment = dfe.getLocalContext()
    return if (env.isPreview) {
      MD_GitMetadata(
        repoName = env.repoKey,
        branch = env.branch,
        pullRequest = MD_PullRequest(
          number = env.pullRequestId,
          link = scmUtils.getPullRequestLink(
            env.repoType,
            env.projectKey,
            env.repoSlug,
            env.pullRequestId
          )
        )
      )
    } else {
      null
    }
  }

  @DgsData(parentType = DgsConstants.MD_APPLICATION.TYPE_NAME, field = DgsConstants.MD_APPLICATION.IsPaused)
  fun isPaused(dfe: DgsDataFetchingEnvironment): Boolean {
    val app: MD_Application = dfe.getSource()
    return actuationPauser.applicationIsPaused(app.name)
  }

  @DgsData(parentType = DgsConstants.MD_APPLICATION.TYPE_NAME, field = DgsConstants.MD_APPLICATION.PausedInfo)
  fun pausedInfo(dfe: DgsDataFetchingEnvironment): MD_PausedInfo? {
    val app: MD_Application = dfe.getSource()
    return actuationPauser.getApplicationPauseInfo(app.name)?.toDgsPaused()
  }

  @DgsData(parentType = DgsConstants.MD_ARTIFACT.TYPE_NAME, field = DgsConstants.MD_ARTIFACT.Versions)
  fun versions(
    dfe: DataFetchingEnvironment,
    @InputArgument("statuses", collectionType = MD_ArtifactStatusInEnvironment::class) statuses: List<MD_ArtifactStatusInEnvironment>?,
    @InputArgument("versions") versionIds: List<String>?,
    @InputArgument("limit") limit: Int?
  ): CompletableFuture<List<DataFetcherResult<MD_ArtifactVersionInEnvironment>>>? {
    val dataLoader: DataLoader<ArtifactAndEnvironment, List<MD_ArtifactVersionInEnvironment>> = dfe.getDataLoader(ArtifactInEnvironmentDataLoader.Descriptor.name)
    val artifact: MD_Artifact = dfe.getSource()
    val config = applicationFetcherSupport.getDeliveryConfigFromContext(dfe)
    val applicationContext: ApplicationContext = DgsContext.getCustomContext(dfe)
    if (statuses != null && applicationContext.requestedStatuses == null) {
      applicationContext.requestedStatuses = statuses.toSet()
    }
    if (versionIds != null && applicationContext.requestedVersionIds == null) {
      applicationContext.requestedVersionIds = versionIds.toSet()
    }
    if (limit != null && applicationContext.requestedLimit == null) {
      applicationContext.requestedLimit = limit
    }

    val deliveryArtifact = config.matchingArtifactByReference(artifact.reference) ?: return null

    return dataLoader.load(
      ArtifactAndEnvironment(
        artifact = deliveryArtifact,
        environmentName = artifact.environment,
      )
    ).thenApply { versions ->
      versions.map { version ->
        DataFetcherResult.newResult<MD_ArtifactVersionInEnvironment>().data(version).localContext(version).build()
      }
    }
  }

  @DgsData(parentType = DgsConstants.MD_GITMETADATA.TYPE_NAME, field = DgsConstants.MD_GITMETADATA.ComparisonLinks)
  fun comparisonLinks(dfe: DataFetchingEnvironment): MD_ComparisonLinks? {
    val diffContext = applicationFetcherSupport.getDiffContext(dfe)

    with(diffContext) {
      return MD_ComparisonLinks(
        toPreviousVersion = if (previousDeployedVersion != fetchedVersion) {
          artifactVersionLinks.generateCompareLink(fetchedVersion.publishedArtifact, previousDeployedVersion?.publishedArtifact, deliveryArtifact)
        } else {
          null
        },
        toCurrentVersion = if (currentDeployedVersion != fetchedVersion) {
          artifactVersionLinks.generateCompareLink(fetchedVersion.publishedArtifact, currentDeployedVersion?.publishedArtifact, deliveryArtifact)
        } else {
          null
        }
      )
    }
  }

  @DgsData(parentType = DgsConstants.MD_ARTIFACTVERSIONINENVIRONMENT.TYPE_NAME, field = DgsConstants.MD_ARTIFACTVERSIONINENVIRONMENT.PackageDiff)
  fun packageDiff(dfe: DataFetchingEnvironment): MD_PackageDiff? {
    return applicationFetcherSupport.getDebianPackageDiff(dfe)
  }

  @DgsData(parentType = DgsConstants.MD_ARTIFACTVERSIONINENVIRONMENT.TYPE_NAME, field = DgsConstants.MD_ARTIFACTVERSIONINENVIRONMENT.LifecycleSteps)
  fun lifecycleSteps(dfe: DataFetchingEnvironment): CompletableFuture<List<MD_LifecycleStep>>? {
    val dataLoader: DataLoader<ArtifactAndVersion, List<MD_LifecycleStep>> = dfe.getDataLoader(LifecycleEventsByVersionDataLoader.Descriptor.name)
    val artifact: MD_ArtifactVersionInEnvironment = dfe.getSource()
    val config = applicationFetcherSupport.getDeliveryConfigFromContext(dfe)
    val deliveryArtifact = config.matchingArtifactByReference(artifact.reference) ?: return null
    return dataLoader.load(
      ArtifactAndVersion(
        deliveryArtifact,
        artifact.version
      )
    )
  }

  @DgsData(parentType = DgsConstants.MD_ARTIFACT.TYPE_NAME, field = DgsConstants.MD_ARTIFACT.PinnedVersion)
  fun pinnedVersion(dfe: DataFetchingEnvironment): CompletableFuture<MD_PinnedVersion>? {
    val dataLoader: DataLoader<PinnedArtifactAndEnvironment, MD_PinnedVersion> = dfe.getDataLoader(PinnedVersionInEnvironmentDataLoader.Descriptor.name)
    val artifact: MD_Artifact = dfe.getSource()
    val config = applicationFetcherSupport.getDeliveryConfigFromContext(dfe)
    val deliveryArtifact = config.matchingArtifactByReference(artifact.reference) ?: return null
    return dataLoader.load(PinnedArtifactAndEnvironment(
      artifact = deliveryArtifact,
      environment = artifact.environment
    ))
  }

  @DgsData(parentType = DgsConstants.MD_APPLICATION.TYPE_NAME, field = DgsConstants.MD_APPLICATION.VersionOnUnpinning)
  fun versionOnUnpinning(dfe: DataFetchingEnvironment,
    @InputArgument("reference") reference: String,
    @InputArgument("environment") environment: String
  ): MD_ArtifactVersionInEnvironment? {
    return getArtifactVersion(dfe, reference, environment)
  }


  @DgsData(parentType = DgsConstants.MD_APPLICATION.TYPE_NAME, field = DgsConstants.MD_APPLICATION.VersionOnRollback)
  fun versionOnRollback(dfe: DataFetchingEnvironment,
    @InputArgument("reference") reference: String,
    @InputArgument("environment") environment: String
  ): MD_ArtifactVersionInEnvironment? {
    //we should exclude artifact versions with status current, as we want to show which version will be
    //deployed if the current version will be rolled back.
    return getArtifactVersion(dfe,  reference, environment, true)
  }


  @DgsData(parentType = DgsConstants.MD_ARTIFACTVERSIONINENVIRONMENT.TYPE_NAME, field = DgsConstants.MD_ARTIFACTVERSIONINENVIRONMENT.Constraints)
  fun artifactConstraints(dfe: DataFetchingEnvironment): CompletableFuture<List<MD_Constraint>>? {
    val dataLoader: DataLoader<EnvironmentArtifactAndVersion, List<MD_Constraint>> = dfe.getDataLoader(ConstraintsDataLoader.Descriptor.name)
    val artifact: MD_ArtifactVersionInEnvironment = dfe.getSource()
    return artifact.environment?.let { environmentName ->
      dataLoader.load(
        EnvironmentArtifactAndVersion(
          environmentName,
          artifact.reference,
          artifact.version
        )
      )
    }
  }

  @DgsData(parentType = DgsConstants.MD_ARTIFACTVERSIONINENVIRONMENT.TYPE_NAME, field = DgsConstants.MD_ARTIFACTVERSIONINENVIRONMENT.Verifications)
  fun artifactVerifications(dfe: DataFetchingEnvironment): CompletableFuture<List<MD_Action>>? {
    val dataLoader: DataLoader<EnvironmentArtifactAndVersion, List<MD_Action>> = dfe.getDataLoader(ActionsDataLoader.Descriptor.name)
    val artifact: MD_ArtifactVersionInEnvironment = dfe.getSource()
    return artifact.environment?.let { environmentName ->
      dataLoader.load(
        EnvironmentArtifactAndVersion(
          environmentName,
          artifact.reference,
          artifact.version,
          ActionType.VERIFICATION
        )
      )
    }
  }

  @DgsData(parentType = DgsConstants.MD_ARTIFACTVERSIONINENVIRONMENT.TYPE_NAME, field = DgsConstants.MD_ARTIFACTVERSIONINENVIRONMENT.PostDeploy)
  fun artifactPostDeploy(dfe: DataFetchingEnvironment): CompletableFuture<List<MD_Action>>? {
    val dataLoader: DataLoader<EnvironmentArtifactAndVersion, List<MD_Action>> = dfe.getDataLoader(ActionsDataLoader.Descriptor.name)
    val artifact: MD_ArtifactVersionInEnvironment = dfe.getSource()
    return artifact.environment?.let { environmentName ->
      dataLoader.load(
        EnvironmentArtifactAndVersion(
          environmentName,
          artifact.reference,
          artifact.version,
          ActionType.POST_DEPLOY
        )
      )
    }
  }

  @DgsData(parentType = DgsConstants.MD_ARTIFACTVERSIONINENVIRONMENT.TYPE_NAME, field = DgsConstants.MD_ARTIFACTVERSIONINENVIRONMENT.Veto)
  fun versionVetoed(dfe: DataFetchingEnvironment): CompletableFuture<MD_VersionVeto?>? {
    val config = applicationFetcherSupport.getDeliveryConfigFromContext(dfe)
    val dataLoader: DataLoader<EnvironmentArtifactAndVersion, MD_VersionVeto?> = dfe.getDataLoader(VetoedDataLoader.Descriptor.name)
    val artifact: MD_ArtifactVersionInEnvironment = dfe.getSource()
    return artifact.environment?.let { environmentName ->
      dataLoader.load(
        EnvironmentArtifactAndVersion(
          environmentName,
          artifact.reference,
          artifact.version,
        )
      )
    }
  }

  /**
   * Fetches the list of dismissible notifications for the application in context.
   */
  @DgsData(parentType = DgsConstants.MD_APPLICATION.TYPE_NAME, field = DgsConstants.MD_APPLICATION.Notifications)
  fun applicationNotifications(dfe: DataFetchingEnvironment): List<MD_Notification>? {
    val config = applicationFetcherSupport.getDeliveryConfigFromContext(dfe)
    return notificationRepository.notificationHistory(config.application, true, setOf(WARNING, ERROR))
      .map { it.toDgs() }
  }

  @DgsData(parentType = DgsConstants.MD_ENVIRONMENT.TYPE_NAME, field = DgsConstants.MD_ENVIRONMENT.IsDeleting)
  fun environmentIsDeleting(dfe: DataFetchingEnvironment): CompletableFuture<Boolean>? {
    val config = applicationFetcherSupport.getDeliveryConfigFromContext(dfe)
    val dataLoader: DataLoader<Environment, Boolean> = dfe.getDataLoader(EnvironmentDeletionStatusLoader.NAME)
    val environment = dfe.getSource<MD_Environment>().let { mdEnv ->
      config.environments.find { it.name == mdEnv.name }
    }
    return environment?.let { dataLoader.load(environment) }
  }

  fun getArtifactVersion(dfe: DataFetchingEnvironment, reference: String, environment: String, excludeCurrent: Boolean? = false): MD_ArtifactVersionInEnvironment? {
    val config = applicationFetcherSupport.getDeliveryConfigFromContext(dfe)
    val deliveryArtifact = config.matchingArtifactByReference(reference) ?: return null
    //[gyardeni + rhorev] please note - some values (like MD_ComparisonLinks) will not be retrieved for MD_ArtifactVersionInEnvironment
    //due to our current dgs model.
    keelRepository.getLatestApprovedInEnvArtifactVersion(config, deliveryArtifact, environment, excludeCurrent)
      ?.let {
        return it.toDgs(environment)
      }
    return null
  }
}


fun Environment.dependsOn(another: Environment) =
  constraints.any { it is DependsOnConstraint && it.environment == another.name }

fun Environment.hasDependencies() =
  constraints.any { it is DependsOnConstraint }
