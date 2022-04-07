package com.netflix.spinnaker.keel.export

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.spinnaker.config.BaseUrlConfig
import com.netflix.spinnaker.config.DefaultWorkhorseCoroutineContext
import com.netflix.spinnaker.config.WorkhorseCoroutineContext
import com.netflix.spinnaker.keel.activation.DiscoveryActivated
import com.netflix.spinnaker.keel.api.AccountAwareLocations
import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.Dependency
import com.netflix.spinnaker.keel.api.DependencyType
import com.netflix.spinnaker.keel.api.Dependent
import com.netflix.spinnaker.keel.api.Exportable
import com.netflix.spinnaker.keel.api.Locatable
import com.netflix.spinnaker.keel.api.Monikered
import com.netflix.spinnaker.keel.api.NotificationConfig
import com.netflix.spinnaker.keel.api.NotificationFrequency
import com.netflix.spinnaker.keel.api.NotificationType
import com.netflix.spinnaker.keel.api.ResourceKind
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.Verification
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.ec2.ApplicationLoadBalancerSpec
import com.netflix.spinnaker.keel.api.ec2.ClassicLoadBalancerSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.ec2.EC2_APPLICATION_LOAD_BALANCER_V1_2
import com.netflix.spinnaker.keel.api.ec2.EC2_CLASSIC_LOAD_BALANCER_V1
import com.netflix.spinnaker.keel.api.ec2.EC2_CLUSTER_V1_1
import com.netflix.spinnaker.keel.api.ec2.EC2_SECURITY_GROUP_V1
import com.netflix.spinnaker.keel.api.ec2.SecurityGroupSpec
import com.netflix.spinnaker.keel.api.migration.SkipReason
import com.netflix.spinnaker.keel.api.plugins.ResourceHandler
import com.netflix.spinnaker.keel.api.plugins.supporting
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.core.api.DEFAULT_SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.core.api.DependsOnConstraint
import com.netflix.spinnaker.keel.core.api.ManualJudgementConstraint
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.core.api.SubmittedEnvironment
import com.netflix.spinnaker.keel.core.api.SubmittedResource
import com.netflix.spinnaker.keel.core.api.TimeWindow
import com.netflix.spinnaker.keel.core.api.AllowedTimesConstraint
import com.netflix.spinnaker.keel.core.api.id
import com.netflix.spinnaker.keel.core.parseMoniker
import com.netflix.spinnaker.keel.exceptions.ArtifactNotSupportedException
import com.netflix.spinnaker.keel.filterNotNullValues
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.model.DeployStage
import com.netflix.spinnaker.keel.front50.model.JenkinsStage
import com.netflix.spinnaker.keel.front50.model.Pipeline
import com.netflix.spinnaker.keel.front50.model.PipelineNotifications
import com.netflix.spinnaker.keel.front50.model.RestrictedExecutionWindow
import com.netflix.spinnaker.keel.jenkins.JenkinsService
import com.netflix.spinnaker.keel.jenkins.PublisherType.HTML_REPORT
import com.netflix.spinnaker.keel.jenkins.PublisherType.JUNIT_REPORT
import com.netflix.spinnaker.keel.notifications.slack.SlackService
import com.netflix.spinnaker.keel.orca.ExecutionDetailResponse
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.persistence.NoDeliveryConfigForApplication
import com.netflix.spinnaker.keel.validators.DeliveryConfigValidator
import com.netflix.spinnaker.keel.verification.jenkins.JenkinsJobVerification
import com.netflix.spinnaker.keel.veto.unhealthy.UnsupportedResourceTypeException
import com.slack.api.model.block.SectionBlock
import com.slack.api.model.block.composition.MarkdownTextObject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.env.Environment
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.DayOfWeek
import java.time.Duration
import java.time.Instant

/**
 * Encapsulates logic to export delivery configs from pipelines.
 */
@Component
class ExportService(
  private val handlers: List<ResourceHandler<*, *>>,
  private val front50Cache: Front50Cache,
  private val orcaService: OrcaService,
  private val baseUrlConfig: BaseUrlConfig,
  private val yamlMapper: YAMLMapper,
  private val validator: DeliveryConfigValidator,
  private val deliveryConfigRepository: DeliveryConfigRepository,
  private val springEnv: Environment,
  private val cloudDriverCache: CloudDriverCache,
  private val jenkinsService: JenkinsService,
  private val slackService: SlackService,
  @Value("\${keel.export.slack-notification-channel}")
  private val slackNotificationChannel: String,
  override val coroutineContext: WorkhorseCoroutineContext = DefaultWorkhorseCoroutineContext
) : CoroutineScope, DiscoveryActivated() {
  private val prettyPrinter by lazy { yamlMapper.writerWithDefaultPrettyPrinter() }

  companion object {
    /**
     * List of pipeline "shapes" that we know how to export. See [Pipeline.shape].
     */
    val EXPORTABLE_PIPELINE_SHAPES = listOf(
      listOf("deploy"),
      listOf("manualJudgment", "deploy"),
      listOf("bake", "deploy", "manualJudgment", "deploy", "manualJudgment", "deploy"),
      listOf("findImage", "deploy", "manualJudgment", "deploy", "manualJudgment", "deploy"),
      listOf("deploy", "manualJudgment", "deploy", "manualJudgment", "deploy"),
      listOf("deploy", "deploy", "deploy"),
      listOf("findImage", "deploy"),
      listOf("findImage", "manualJudgement", "deploy"),
      listOf("findImageFromTags", "deploy"),
      listOf("findImageFromTags", "manualJudgement", "deploy"),
      listOf("bake", "deploy")
    )

    /**
     * List of pipeline shapes that we know how to convert to a [Verification].
     */
    val EXPORTABLE_TEST_PIPELINE_SHAPES = listOf(
      listOf("jenkins"),
      listOf("jenkins", "manualJudgment"),
      listOf("wait", "jenkins"),
      listOf("wait", "jenkins", "manualJudgment")
    )

    val SUPPORTED_TRIGGER_TYPES = listOf("docker", "jenkins", "rocket", "pipeline")

    val PROVIDERS_TO_CLUSTER_KINDS = mapOf(
      "aws" to EC2_CLUSTER_V1_1.kind,
      "titus" to TITUS_CLUSTER_V1.kind
    )

    val SPEL_REGEX = Regex("\\$\\{.+\\}")

    const val PRODUCTION_ENV: String = "production"
    const val TESTING_ENV = "testing"
  }

  private val isScheduledExportEnabled: Boolean
    get() = springEnv.getProperty("keel.pipelines-export.scheduled.is-enabled", Boolean::class.java, false)

  private val exportMinAge: Duration
    get() = springEnv.getProperty("keel.pipelines-export.scheduled.min-age-duration", Duration::class.java, Duration.ofDays(1))

  private val exportBatchSize: Int
    get() = springEnv.getProperty("keel.pipelines-export.scheduled.batch-size", Int::class.java, 5)

  private val shouldExportVerifications: Boolean
    get() = springEnv.getProperty("keel.pipelines-export.scheduled.export-verifications", Boolean::class.java, false)

  /**
   * Converts a [cloudProvider] and resource [type] passed to an export function to a [ResourceKind].
   */
  private fun typeToKind(cloudProvider: String, type: String): ResourceKind =
    when (type) {
      "classicloadbalancer" -> EC2_CLASSIC_LOAD_BALANCER_V1.kind
      "classic-load-balancer" -> EC2_CLASSIC_LOAD_BALANCER_V1.kind
      "classicloadbalancers" -> EC2_CLASSIC_LOAD_BALANCER_V1.kind
      "applicationloadbalancer" -> EC2_APPLICATION_LOAD_BALANCER_V1_2.kind
      "applicationloadbalancers" -> EC2_APPLICATION_LOAD_BALANCER_V1_2.kind
      "security-group" -> EC2_SECURITY_GROUP_V1.kind
      "securitygroup" -> EC2_SECURITY_GROUP_V1.kind
      "securitygroups" -> EC2_SECURITY_GROUP_V1.kind
      "cluster" -> PROVIDERS_TO_CLUSTER_KINDS[cloudProvider] ?: error("Cloud provider $cloudProvider is not supported")
      "clusters" -> PROVIDERS_TO_CLUSTER_KINDS[cloudProvider] ?: error("Cloud provider $cloudProvider is not supported")
      else -> error("Unsupported resource type $cloudProvider:$type")
    }

  /**
   * @return An [Exportable] object representing the resource of the specified [kind] and [name] in the specified [account].
   */
  @Suppress("UNCHECKED_CAST")
  private fun generateExportable(kind: ResourceKind, account: String, name: String, user: String): Exportable {
    return Exportable(
      cloudProvider = kind.group,
      kind = kind,
      account = account,
      user = user,
      moniker = parseMoniker(name),
      regions = (
        cloudDriverCache
          .credentialBy(account)
          .attributes["regions"] as List<Map<String, Any>>
        )
        .map { it["name"] as String }
        .toSet(),
    )
  }

  /**
   * @return A [SubmittedResource] based on the current state of an existing resource in the cloud, as identified
   * by the specified [cloudProvider], resource [type], [account] and resource [name].
   */
  suspend fun exportResource(
    cloudProvider: String,
    type: String,
    account: String,
    name: String,
    user: String
  ): SubmittedResource<ResourceSpec> {
    val kind = typeToKind(cloudProvider, type)
    val handler = handlers.supporting(kind)
    val exportable = generateExportable(kind, account, name, user)

    log.info("Exporting resource ${exportable.toResourceId()}")
    return SubmittedResource(
      kind = kind,
      spec = handler.export(exportable)
    )
  }

  /**
   * @return A [SubmittedResource] based on the current state of the existing [resource] in the cloud.
   * If this method is called consecutively without any changes having occurred to the state of the resource,
   * it is expected to return an object equal to the one passed in. If the state has changed, the differences
   * will be reflected in the returned object, which can be useful for comparing a previous version of an
   * exported resource with a fresh export, hence the name "re-export".
   */
  suspend fun reExportResource(resource: SubmittedResource<*>, user: String): SubmittedResource<*> {
    val moniker = (resource.spec as? Monikered)?.moniker
      ?: throw IllegalArgumentException("We can only export resources with a moniker currently.")

    val locations = (resource.spec as? Locatable<*>)?.let {
      (it.locations as? AccountAwareLocations<*>)
    } ?: throw IllegalArgumentException("We can only export resources with locations currently.")

    val handler = handlers.supporting(resource.kind)
    val exportable = Exportable(
      cloudProvider = resource.kind.group,
      account = locations.account,
      moniker = moniker,
      regions = locations.regions.map { it.name }.toSet(),
      kind = resource.kind,
      user = user
    )

    log.info("Re-exporting resource ${resource.id}")
    return SubmittedResource(
      kind = resource.kind,
      spec = handler.export(exportable)
    )
  }

  /**
   * @return A [DeliveryArtifact] representing the software artifact currently deployed to the cluster identified
   * by [cloudProvider], [account] and [clusterName], if any, or null otherwise.
   */
  suspend fun exportArtifact(cloudProvider: String, account: String, clusterName: String, user: String): DeliveryArtifact? {
    val kind = typeToKind(cloudProvider, "cluster")
    val handler = handlers.supporting(kind)
    val exportable = generateExportable(kind, account, clusterName, user)

    log.info("Exporting artifact from cluster ${exportable.toResourceId()}")
    return handler.exportArtifact(exportable)
  }

  /**
   * Run [exportFromPipelines] on all available apps and store their results
   */
  @Scheduled(fixedDelayString = "\${keel.pipelines-export.scheduled.frequency:PT5M}")
  fun checkAppsForExport() {
    if (isScheduledExportEnabled && enabled.get()) {
      val apps = deliveryConfigRepository.getAppsToExport(exportMinAge, exportBatchSize)
      log.debug("Running the migration export on apps: $apps")
      val jobs = apps.map { app ->
        launch {
          try {
            when (val result = exportFromPipelines(app, includeVerifications = shouldExportVerifications)) {
              is ExportErrorResult -> deliveryConfigRepository.storeFailedPipelinesExportResult(app, result.error)
              is ExportSkippedResult -> log.info("Skipping export of $app - it is already on MD")
              is PipelineExportResult -> deliveryConfigRepository.storePipelinesExportResult(
                deliveryConfig = result.deliveryConfig,
                skippedPipelines = result.toSkippedPipelines(),
                exportSucceeded = result.exportSucceeded,
                repoSlug = result.repoSlug,
                projectKey = result.projectKey,
                isInactive = result.isInactive
              )
            }
          } catch (e: Exception) {
            log.error("Failed to export application $app")
          }
        }
      }
      runBlocking { jobs.joinAll() }
      log.debug("Finished running the migration export on apps: $apps")
    }
  }

  /**
   * Given an application name, look up all the associated pipelines and attempt to build a delivery config
   * that represents the corresponding environments, artifacts and delivery flow.
   *
   * Supports only a sub-set of well-known pipeline patterns (see [EXPORTABLE_PIPELINE_SHAPES]).
   */
  suspend fun exportFromPipelines(
    applicationName: String,
    maxAgeDays: Long = 6L * 30L,
    includeVerifications: Boolean = false
  ): ExportResult {
    log.info("Exporting delivery config from pipelines for application $applicationName (max age: $maxAgeDays days)")

    val pipelines = front50Cache.pipelinesByApplication(applicationName)
    val application = front50Cache.applicationByName(applicationName)
    val serviceAccount = application.email ?: DEFAULT_SERVICE_ACCOUNT

    try {
      deliveryConfigRepository.getByApplication(applicationName)
      log.debug("Found an existing config for application $applicationName. Will not proceed with export process.")
      return ExportSkippedResult(isManaged = true)
    } catch (e: NoDeliveryConfigForApplication) {
      log.debug("Did not find an existing config for application $applicationName. Trying to export...")
    }

    // 1. find pipelines which are not matching to the supported patterns above
    var nonExportablePipelines = pipelines.findNonExportable(maxAgeDays, includeVerifications)

    // 2. get the actual supported pipelines
    val exportablePipelines = pipelines - nonExportablePipelines.keys

    // this returns a 1:1 mapping between a "deploy" stage and the associated server group
    val pipelinesToDeploysAndClusters = exportablePipelines.toDeploysAndClusters(serviceAccount)

    val environments = mutableSetOf<SubmittedEnvironment>()
    val artifacts = mutableSetOf<DeliveryArtifact>()
    var configValidationException: Exception? = null

    // 3. iterate over the deploy pipelines, and map resources to the environments they represent
    val deployPipelinesToEnvironments = pipelinesToDeploysAndClusters.mapValues { (pipeline, deploysAndClusters) ->
      // 4. iterate over the clusters, export their resources, create constraints and environment
      deploysAndClusters.mapNotNull { (deploy, cluster) ->
        log.debug("Attempting to build environment for cluster ${cluster.moniker}")
        val environmentName = inferEnvironmentName(cluster)
        val manualJudgement = if (pipeline.hasManualJudgment(deploy)) {
          log.debug("Adding manual judgment constraint for environment $environmentName based on manual judgment stage in pipeline ${pipeline.name}")
          setOf(ManualJudgementConstraint())
        } else {
          emptySet()
        }

        val allowedTimes = createAllowedTimeConstraint(deploy.restrictedExecutionWindow)

        // 5. infer from the cluster all dependent resources, including artifacts
        val resourcesAndArtifacts = try {
          createResourcesBasedOnCluster(cluster, environments, applicationName)
        } catch (e: ArtifactNotSupportedException) {
          log.debug("could not export artifacts from cluster ${cluster.moniker.toName()}")
          nonExportablePipelines = nonExportablePipelines + (pipeline to SkipReason.ARTIFACT_NOT_SUPPORTED)
          return@mapValues emptyList()
        }

        if (resourcesAndArtifacts.resources.isEmpty()) {
          log.debug("could not create resources from cluster ${cluster.moniker.toName()}")
          nonExportablePipelines = nonExportablePipelines + (pipeline to SkipReason.RESOURCE_NOT_SUPPORTED)
          return@mapValues emptyList()
        }

        // 6. export verifications from this pipeline
        val verifications = if (includeVerifications) {
          try {
            pipeline.exportVerifications(deploy)
          } catch (e: UnsupportedJenkinsStage) {
            log.debug("Failed to export verifications due to unsupported Jenkins stage: $e")
            return@mapValues emptyList()
          }
        } else {
          emptyList()
        }

        val environment = SubmittedEnvironment(
          name = environmentName,
          resources = resourcesAndArtifacts.resources,
          constraints = manualJudgement + allowedTimes,
          verifyWith = verifications
        )

        log.debug("Exported environment $environmentName from cluster [${cluster.moniker}]")

        environments.add(environment)

        val artifact = resourcesAndArtifacts.artifact
        if (artifact != null) {
          artifacts.add(artifact)
        } else {
          log.debug("couldn't find any artifact for cluster ${cluster.moniker.toName()}")
        }

        environment
      }
    }

    // 7. find test pipelines
    val testPipelines = pipelines.filter { it.shape in EXPORTABLE_TEST_PIPELINE_SHAPES }
    val testPipelinesToEnvironments = mutableMapOf<Pipeline, List<SubmittedEnvironment>>()
    var pipelineNotifications = listOf<PipelineNotifications>()
    var deliveryConfig = SubmittedDeliveryConfig(
      application = applicationName,
      name = applicationName,
      serviceAccount = serviceAccount
    )

    // 8. assemble the final environments
    if (deployPipelinesToEnvironments.isNotEmpty()) {
      val finalEnvironments = deployPipelinesToEnvironments.flatMap { (deployPipeline, envs) ->
        envs.map { environment ->
          // 8a. look at the pipeline triggers and dependencies between pipelines to find any additional constraints
          val additionalConstraints = exportConstraintsFromTriggers(applicationName, deployPipeline, environment, deployPipelinesToEnvironments)

          // 8b. look at test pipelines independent of the deployment pipeline to find any additional verifications
          val additionalVerifications = if (includeVerifications) {
            testPipelines.exportVerifications(deployPipeline).onEach { (testPipeline, _) ->
              testPipelinesToEnvironments.compute(testPipeline) { _, curEnvs ->
                (curEnvs ?: mutableListOf()) + environment
              }
            }
          } else {
            emptyMap()
          }

          pipelineNotifications = pipelineNotifications + deployPipeline.notifications
          environment.copy(
            constraints = environment.constraints + additionalConstraints,
            notifications = notifications(environment.name, application.slackChannel?.name, deployPipeline.notifications),
            verifyWith = environment.verifyWith + additionalVerifications.values
          ).addMetadata(
            "exportedFrom" to deployPipeline.link(baseUrlConfig.baseUrl)
          )
        }
      }.toSet()
        .dedupe(application.slackChannel?.name, pipelineNotifications)

      val finalArtifacts = artifacts.dedupe()

      deliveryConfig = SubmittedDeliveryConfig(
        name = applicationName,
        application = applicationName,
        serviceAccount = serviceAccount,
        artifacts = finalArtifacts,
        environments = finalEnvironments.sensibleOrder(),
        metadata = mapOf(
          "migrating" to true
        )
      )

      try {
        validator.validate(deliveryConfig)
      } catch (ex: Exception) {
        log.debug("Delivery config for application ${application.name} didn't passed validation; exception: $ex", ex)
        configValidationException = ex
      }
    }

    val result = PipelineExportResult(
      deliveryConfig = deliveryConfig,
      // include the test pipelines that exported verifications
      processed = deployPipelinesToEnvironments + testPipelinesToEnvironments,
      skipped = nonExportablePipelines,
      baseUrl = baseUrlConfig.baseUrl,
      configValidationException = configValidationException?.message,
      repoSlug = application.repoSlug,
      projectKey = application.repoProjectKey
    )

    log.info("Successfully exported delivery config:\n${prettyPrinter.writeValueAsString(result)}")
    return result
  }

  private fun createAllowedTimeConstraint(executionWindow: RestrictedExecutionWindow?) =
    if (executionWindow != null) {
      val timeWindow = executionWindow.whitelist?.joinToString(",") { time ->
        "${time.startHour}-${time.endHour}"
      }
      setOf(AllowedTimesConstraint(
        windows = listOf(
          TimeWindow(
            days = executionWindow.days?.joinToString(",") {
              it.fromDayNumberToWeekday()
            },
            hours = timeWindow
          )
        )
      ))
    } else {
      emptySet()
    }

  /**
   * Adding Slack notifications for production environment, using the Slack channel as defined in spinnaker's configuration.
   * If no Slack channel is defined, use the pipeline's Slack notifications
   */
  private fun notifications(envName: String, slackChannel: String?, notifications: List<PipelineNotifications>): Set<NotificationConfig> {
    if (envName == PRODUCTION_ENV) {
      return if (slackChannel != null) { // if the Slack channel explicitly defined, use it
        setOf(NotificationConfig(
          address = slackChannel,
          type = NotificationType.slack,
          frequency = NotificationFrequency.normal
        ))
      } else {
        notifications.filter { it.type == "slack" }
          .map {
            NotificationConfig(
              address = it.address,
              type = NotificationType.slack,
              frequency = NotificationFrequency.normal
            )
          }.toSet()
      }
    }
    return emptySet()
  }

  private fun inferEnvironmentName(cluster: Exportable): String {
    return with(cluster) {
      when {
        !moniker.stack.isNullOrEmpty() -> moniker.stack!!
        "test" in account -> TESTING_ENV
        "prod" in account -> PRODUCTION_ENV
        else -> account
      }
    }
  }

  /**
   * Finds non-exportable pipelines in the list and associates them with the reason why they're not.
   */
  private fun List<Pipeline>.findNonExportable(maxAgeDays: Long, includeVerifications: Boolean): Map<Pipeline, SkipReason> {
    val lastExecutions = runBlocking {
      associateWith { pipeline ->
        //return a map with the pipeline and its latest execution
        orcaService.getExecutions(pipeline.id).firstOrNull()
      }
    }

    return associateWith { pipeline ->
      val lastExecution = lastExecutions[pipeline]
      log.debug("Checking pipeline [${pipeline.name}], last execution: ${lastExecution?.buildTime}")

      when {
        pipeline.disabled -> SkipReason.DISABLED
        (lastExecution == null || lastExecution.olderThan(maxAgeDays)) -> SkipReason.NOT_EXECUTED_RECENTLY
        pipeline.fromTemplate -> SkipReason.FROM_TEMPLATE
        pipeline.hasParallelStages -> SkipReason.HAS_PARALLEL_STAGES
        !isExportable(pipeline, includeVerifications) -> SkipReason.SHAPE_NOT_SUPPORTED
        else -> null
      }
    }.filterNotNullValues()
  }

  fun isExportable(pipeline: Pipeline, includeVerifications: Boolean = false): Boolean =
    EXPORTABLE_PIPELINE_SHAPES.let { basicShapes ->
      if (includeVerifications) {
        // also support all the shapes above followed by a jenkins stage supposedly used for tests
        basicShapes + basicShapes.map { shape -> shape + "jenkins" }
      } else {
        basicShapes
      }
    }.contains(pipeline.shape)

  // Take a cluster exportable and return a pair with its dependent resources specs (cluster, security groups, and load balancers, and the artifact).
  private suspend fun createResourcesBasedOnCluster(
    cluster: Exportable,
    environments: Set<SubmittedEnvironment>,
    applicationName: String
  ): Pair<Set<SubmittedResource<ResourceSpec>>, DeliveryArtifact?> {
    val spec = try {
      handlers.supporting(cluster.clusterKind).export(cluster)
    } catch (e: Exception) {
      log.error("Unable to export cluster ${cluster.moniker}: $e. Ignoring.")
      return Pair(emptySet(), null)
    }

    // Export the artifact matching the cluster while we're at it
    val artifact = try {
      handlers.supporting(cluster.clusterKind).exportArtifact(cluster)
    } catch (e: ArtifactNotSupportedException) {
      deliveryConfigRepository.updateMigratingAppScmStatus(applicationName, false)
      log.error("artifact for application $applicationName is not supported")
      throw e
    }

    deliveryConfigRepository.updateMigratingAppScmStatus(applicationName, true)
    log.debug("Exported artifact $artifact from cluster ${cluster.moniker}")
    // get the cluster dependencies
    val dependencies = spec as Dependent


    // Get all the cluster dependencies security groups and alb
    val securityGroupResources = dependentResources(dependencies, environments, cluster, EC2_SECURITY_GROUP_V1.kind, applicationName)
    val loadBalancersResources = dependentResources(dependencies, environments, cluster, EC2_CLASSIC_LOAD_BALANCER_V1.kind, applicationName)
    val targetGroupResources = dependentResources(dependencies, environments, cluster, EC2_APPLICATION_LOAD_BALANCER_V1_2.kind, applicationName)

    return Pair(
      // combine dependent resources and cluster resource
      loadBalancersResources + securityGroupResources + targetGroupResources +
        setOf(SubmittedResource(
          kind = cluster.clusterKind,
          spec = when (spec) {
            is ClusterSpec -> {
              spec.copy(
                artifactReference = artifact.reference
              )
            }
            else -> spec
          }
        )), artifact)
  }

  private suspend fun dependentResources(
    dependencies: Dependent,
    environments: Set<SubmittedEnvironment>,
    cluster: Exportable,
    kind: ResourceKind,
    applicationName: String
  ): Set<SubmittedResource<ResourceSpec>> {

    val dependencyType = when (kind) {
      EC2_SECURITY_GROUP_V1.kind -> DependencyType.SECURITY_GROUP
      EC2_CLASSIC_LOAD_BALANCER_V1.kind -> DependencyType.LOAD_BALANCER
      EC2_APPLICATION_LOAD_BALANCER_V1_2.kind -> DependencyType.TARGET_GROUP
      else -> throw UnsupportedResourceTypeException("Kind $kind is not supported")
    }

    // fetch the cluster dependencies (i.e security groups)
    val clusterDependencies: List<Dependency> = dependencies.dependsOn.filter { it.type == dependencyType }

    val resourcesSpec = mutableSetOf<ResourceSpec>()
    val account = when (cluster.account) {
      "titustestvpc" -> "test"
      "titusprodvpc" -> "prod"
      else -> cluster.account
    }

    // Get the exportable for all security groups
    clusterDependencies.forEach {
      val resourceName = it.name
      if (!resourceName.startsWith(applicationName)) {
        log.debug("resource ${it.name} doesn't start with the application name. will not create it.")
      }
      // Check if we already exported any security group or load balancers when creating previous environments
      else if ((kind == EC2_SECURITY_GROUP_V1.kind && checkIfSecurityGroupExists(account, resourceName, environments)) ||
        (kind == EC2_APPLICATION_LOAD_BALANCER_V1_2.kind && checkIfApplicationLBExists(account, resourceName, environments)) ||
        (kind == EC2_CLASSIC_LOAD_BALANCER_V1.kind && checkIfClassicLBExists(account, resourceName, environments))) {
        log.debug("found an existing resource with name $resourceName, will not recreate it.")
      } else {
        try {
          val resourceSpec = handlers.supporting(kind).export(
            //copy everything from the cluster and just change the type
            cluster.copy(
              kind = kind,
              moniker = parseMoniker(resourceName),
              account = account
            )
          )
          resourcesSpec.add(resourceSpec)
        } catch (ex: Exception) {
          log.debug("could not export $resourceName", ex)
        }
      }
    }

    // create the actual resources from the exportable
    return resourcesSpec.map {
      SubmittedResource(
        kind = kind,
        spec = it
      )
    }.toSet()
  }

  private fun checkIfSecurityGroupExists(account: String, name: String, environments: Set<SubmittedEnvironment>): Boolean {
    environments.map { environment ->
      environment.resources.filter {
        it.spec is SecurityGroupSpec
      }.map {
        val sg = it.spec as SecurityGroupSpec
        if (sg.locations.account == account && sg.moniker.toName() == name) {
          return true
        }
      }
    }
    return false
  }

  private fun checkIfClassicLBExists(account: String, name: String, environments: Set<SubmittedEnvironment>): Boolean {
    environments.map { environment ->
      environment.resources.filter {
        it.spec is ClassicLoadBalancerSpec
      }.map {
        val clb = it.spec as ClassicLoadBalancerSpec
        if (clb.locations.account == account && clb.moniker.toName() == name) {
          return true
        }
      }
    }
    return false
  }

  private fun checkIfApplicationLBExists(account: String, name: String, environments: Set<SubmittedEnvironment>): Boolean {
    environments.map { environment ->
      environment.resources.filter {
        it.spec is ApplicationLoadBalancerSpec
      }.map {
        val alb = it.spec as ApplicationLoadBalancerSpec
        if (alb.locations.account == account && alb.moniker.toName() == name) {
          return true
        }
      }
    }
    return false
  }

  /**
   * Extracts cluster information from deploy stages in the list of pipelines, and returns the pipelines
   * mapped to those deploy stages and respective clusters represented as [Exportable] objects.
   */
  private fun List<Pipeline>.toDeploysAndClusters(serviceAccount: String) =
    associateWith { pipeline ->
      pipeline
        .stages
        .filterIsInstance<DeployStage>()
        .flatMap { deploy ->
          deploy
            .clusters
            .groupBy { "${it.provider}/${it.account}/${it.name}" }
            .map { (key, clusters) ->
              val regions = clusters.mapNotNull { it.region }.toSet()
              val (provider, account, name) = key.split("/")
              deploy to Exportable(
                cloudProvider = provider,
                account = account,
                moniker = parseMoniker(name.replace(SPEL_REGEX, "REDACTED_SPEL")),
                regions = regions,
                kind = PROVIDERS_TO_CLUSTER_KINDS[provider]!!,
                user = serviceAccount
              )
            }
        }
    }

  /**
   * Attempts to extract [Constraint]s from the pipeline triggers for the specified [environment].
   */
  private fun exportConstraintsFromTriggers(
    application: String,
    pipeline: Pipeline,
    environment: SubmittedEnvironment,
    pipelinesToEnvironments: Map<Pipeline, List<SubmittedEnvironment>?>
  ): Set<Constraint> {
    val constraints = mutableSetOf<Constraint>()

    val triggers = pipeline.triggers
      .filter { it.type in SUPPORTED_TRIGGER_TYPES && it.enabled }

    if (triggers.isEmpty()) {
      // if there's no supported trigger, the pipeline is triggered manually, i.e. the equivalent of a manual judgment
      log.debug("Pipeline '${pipeline.name}' for environment ${environment.name} has no supported triggers enabled. " +
        "Adding manual-judgment constraint.")
      constraints.add(ManualJudgementConstraint())
      return constraints
    }

    triggers.forEach { trigger ->
      when (trigger.type) {
        "docker", "jenkins", "rocket" -> {
          log.debug("Pipeline '${pipeline.name}' for environment ${environment.name} has CI trigger. " +
            "This will be handled automatically by artifact detection and approval.")
        }
        "pipeline" -> {
          // if trigger is a pipeline trigger, find the upstream environment matching that pipeline to make a depends-on
          // constraint
          val upstreamEnvironment = pipelinesToEnvironments.entries.find { (pipeline, _) ->
            application == trigger.application && pipeline.id == trigger.pipeline
          }
            ?.let { (_, envs) ->
              // use the last environment within the matching pipeline (which would match the last deploy,
              // in case there's more than one)
              envs?.last()
            }

          if (upstreamEnvironment != null) {
            log.debug("Pipeline '${pipeline.name}' for environment ${environment.name} has pipeline trigger. " +
              "Adding matching depends-on constraint on upstream environment ${upstreamEnvironment.name}.")
            constraints.add(DependsOnConstraint(upstreamEnvironment.name))
          } else {
            log.warn("Pipeline '${pipeline.name}' for environment ${environment.name} has pipeline trigger, " +
              "but upstream environment not found. Adding manual-judgement constraint.")
            constraints.add(ManualJudgementConstraint())
          }
        }
        else -> log.warn("Ignoring unsupported trigger type ${trigger.type} in pipeline '${pipeline.name}' for export")
      }
    }

    return constraints
  }

  /**
   * Removes duplicate environments in the set by merging together environments with the same name.
   */
  @JvmName("dedupeEnvironments")
  private fun Set<SubmittedEnvironment>.dedupe(slackChannel: String?, pipelineNotifications: List<PipelineNotifications>): Set<SubmittedEnvironment> {
    val dedupedEnvironments = mutableSetOf<SubmittedEnvironment>()
    forEach { environment ->
      val previouslyFound = dedupedEnvironments.find { it.name == environment.name }
      val dedupedEnvironment = when {
        previouslyFound != null -> {
          log.debug("Merging matching environments $environment and $previouslyFound")
          dedupedEnvironments.remove(previouslyFound)
          previouslyFound.run {
            copy(
              resources = resources + environment.resources,
              constraints = constraints + environment.constraints
            )
          }
        }
        else -> environment
      }
      dedupedEnvironments.add(dedupedEnvironment)
    }
    val noNotificationsSoFar = dedupedEnvironments.all {
      it.notifications.isEmpty()
    }
    if (noNotificationsSoFar && dedupedEnvironments.isNotEmpty()) {
      val firstEnv = dedupedEnvironments.first()
      dedupedEnvironments.remove(firstEnv)
      val firstEnvWithNotifications = firstEnv.copy(
        //Sending here "PRODUCTION_ENV" to bypass the condition at the notification function. We can fix it later to be more generic.
        notifications = notifications(PRODUCTION_ENV, slackChannel, pipelineNotifications)
      )
      dedupedEnvironments.add(firstEnvWithNotifications)
    }
    return dedupedEnvironments
  }

  /**
   * Removes duplicate artifacts in the set by merging together artifacts with the same type, name and origin.
   */
  private fun Set<DeliveryArtifact>.dedupe(): Set<DeliveryArtifact> {
    val dedupedArtifacts = mutableSetOf<DeliveryArtifact>()
    forEach { artifact ->
      val previouslyFound = dedupedArtifacts.find { it.type == artifact.type && it.name == artifact.name && it.from == artifact.from }
      val dedupedArtifact = when {
        previouslyFound is DebianArtifact && artifact is DebianArtifact -> {
          log.debug("Merging matching artifacts $artifact and $previouslyFound")
          dedupedArtifacts.remove(previouslyFound)
          previouslyFound.run {
            copy(
              vmOptions = vmOptions.copy(regions = vmOptions.regions + artifact.vmOptions.regions)
            )
          }
        }
        else -> artifact
      }
      dedupedArtifacts.add(dedupedArtifact)
    }
    return dedupedArtifacts
  }

  /**
   * Attempts to order the set of environments in a more sensible fashion than just randomly.
   */
  private fun Set<SubmittedEnvironment>.sensibleOrder() =
    toSortedSet { env1, env2 ->
      val env1DependsOn = env1.constraints.filterIsInstance<DependsOnConstraint>().firstOrNull()
      val env2DependsOn = env2.constraints.filterIsInstance<DependsOnConstraint>().firstOrNull()
      when {
        env1DependsOn?.environment == env2.name -> 1
        env2DependsOn?.environment == env1.name -> -1
        else -> when {
          env1.constraints.isEmpty() -> -1
          env2.constraints.isEmpty() -> 1
          else -> env1.name.compareTo(env2.name)
        }
      }
    }

  private fun ExecutionDetailResponse.olderThan(days: Long) =
    buildTime.isBefore(Instant.now() - Duration.ofDays(days))

  /**
   * Given a [Pipeline], look for supported test stages following a [DeployStage] and convert each to
   * a [Verification].
   *
   * Currently, only Jenkins stages are supported.
   */
  private suspend fun Pipeline.exportVerifications(deployStage: DeployStage): List<Verification> {
    // find Jenkins stages that come after the deploy stage (if they come before, they're not being used to run tests)
    val jenkinsStages = findDownstreamJenkinsStages(deployStage)
    return jenkinsStages.mapNotNull { exportVerification(this, it) }
  }

  /**
   * Given a list of test pipelines, look for those triggered by the specified [deployPipeline] and converts any
   * supported test stages in them to a [Verification].
   */
  private suspend fun List<Pipeline>.exportVerifications(deployPipeline: Pipeline): Map<Pipeline, Verification> {
    return associateWith { testPipeline ->
      val triggerPipelineId = testPipeline.triggers
        .find { it.type.lowercase() == "pipeline" }
        ?.let { it.pipeline }

      if (deployPipeline.id == triggerPipelineId) {
        testPipeline.exportVerifications()
      } else {
        null
      }
    }.filterNotNullValues()
  }

  /**
   * Converts a test [Pipeline] to a [Verification], but only if it contains a [JenkinsStage] that looks like it's used
   * for running tests.
   */
  private suspend fun Pipeline.exportVerifications(): Verification? =
    stages.filterIsInstance<JenkinsStage>().firstOrNull()?.let { jenkinsStage ->
      exportVerification(this, jenkinsStage)
    }

  /**
   * Converts a [JenkinsStage] to a [Verification], but only if it looks like it's used for running tests
   * by inspecting its "publishers" and checking for the presence of HTML and/or JUnit reports.
   *
   * @throws UnsupportedJenkinsStage if the Jenkins stage contains parameters with SpEL expressions.
   */
  private suspend fun exportVerification(pipeline: Pipeline, jenkinsStage: JenkinsStage): Verification? {
    val looksLikeTests = try {
      val config = jenkinsService.getJobConfig(jenkinsStage.controller, jenkinsStage.job)
      config.project.publishers.any { it.type == JUNIT_REPORT || it.type == HTML_REPORT }
    } catch (e: Exception) {
      log.debug("Error retrieving/parsing config for Jenkins job ${jenkinsStage.controller}/${jenkinsStage.job}: $e")
      false
    }

    if (!looksLikeTests) {
      log.debug("Jenkins job ${jenkinsStage.job} in pipeline ${pipeline.name} for application ${pipeline.application}" +
        " does not look like it's running any tests (no test reports found)."
      )
      return null
    }

    val parametersWithSpel = jenkinsStage.parameters.filter { (_, value) ->
      SPEL_REGEX.matches(value)
    }

    if (parametersWithSpel.isNotEmpty()) {
      val message = "Found parameters with SpEL expression for Jenkins job `${jenkinsStage.job}`" +
        " in pipeline `${pipeline.name}` for application `${pipeline.application}`:\n" +
        parametersWithSpel.entries.joinToString { (key, value) -> "  - $key: $value\n" } +
        "Link: ${pipeline.link(baseUrlConfig.baseUrl)}"
      log.debug(message)
      slackService.notifyTeam(message)
      throw UnsupportedJenkinsStage(message)
    }

    return JenkinsJobVerification(
      controller = jenkinsStage.controller,
      job = jenkinsStage.job,
      staticParameters = jenkinsStage.parameters
      // TODO: should we try to setup some dynamic parameters for simple/common SpEL expressions?
    )
  }

  private fun SlackService.notifyTeam(message: String) {
    val block = SectionBlock().apply { text = MarkdownTextObject(":alert: $message", false) }
    postChatMessage(slackNotificationChannel, listOf(block), message)
  }
}

private val Exportable.clusterKind: ResourceKind
  get() = ExportService.PROVIDERS_TO_CLUSTER_KINDS[this.cloudProvider]
    ?: error("Unsupported cluster cloud provider '${this.cloudProvider}'")

private val Pair<Set<SubmittedResource<ResourceSpec>>, DeliveryArtifact?>.artifact: DeliveryArtifact?
  get() = this.second

private val Pair<Set<SubmittedResource<ResourceSpec>>, DeliveryArtifact?>.resources: Set<SubmittedResource<ResourceSpec>>
  get() = this.first

private fun Int.fromDayNumberToWeekday(): String =
  when (this) {
    1 -> DayOfWeek.SUNDAY
    2 -> DayOfWeek.MONDAY
    3 -> DayOfWeek.TUESDAY
    4 -> DayOfWeek.WEDNESDAY
    5 -> DayOfWeek.THURSDAY
    6 -> DayOfWeek.FRIDAY
    else -> DayOfWeek.SATURDAY
  }.toString()
