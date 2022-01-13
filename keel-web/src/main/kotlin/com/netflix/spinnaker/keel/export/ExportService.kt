package com.netflix.spinnaker.keel.export

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.spinnaker.config.BaseUrlConfig
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
import com.netflix.spinnaker.keel.api.migration.SkippedPipeline
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
import com.netflix.spinnaker.keel.core.api.TimeWindowConstraint
import com.netflix.spinnaker.keel.core.api.id
import com.netflix.spinnaker.keel.core.parseMoniker
import com.netflix.spinnaker.keel.filterNotNullValues
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.model.DeployStage
import com.netflix.spinnaker.keel.front50.model.Pipeline
import com.netflix.spinnaker.keel.front50.model.RestrictedExecutionWindow
import com.netflix.spinnaker.keel.igor.JobService
import com.netflix.spinnaker.keel.orca.ExecutionDetailResponse
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.persistence.NoDeliveryConfigForApplication
import com.netflix.spinnaker.keel.validators.DeliveryConfigValidator
import com.netflix.spinnaker.keel.veto.unhealthy.UnsupportedResourceTypeException
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
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
@EnableConfigurationProperties(ScmConfig::class)
class ExportService(
  private val handlers: List<ResourceHandler<*, *>>,
  private val front50Cache: Front50Cache,
  private val orcaService: OrcaService,
  private val baseUrlConfig: BaseUrlConfig,
  private val yamlMapper: YAMLMapper,
  private val validator: DeliveryConfigValidator,
  private val deliveryConfigRepository: DeliveryConfigRepository,
  private val jobService: JobService,
  private val springEnv: Environment,
  private val scmConfig: ScmConfig,
  private val cloudDriverCache: CloudDriverCache
) {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }
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

    val SUPPORTED_TRIGGER_TYPES = listOf("docker", "jenkins", "rocket", "pipeline")

    val PROVIDERS_TO_CLUSTER_KINDS = mapOf(
      "aws" to EC2_CLUSTER_V1_1.kind,
      "titus" to TITUS_CLUSTER_V1.kind
    )

    val SPEL_REGEX = Regex("\\$\\{.+\\}")

    val PRODUCTION_EVN = "production"
    val TESTING_EVN = "testing"
  }

  private val isScheduledExportEnabled : Boolean
    get() = springEnv.getProperty("keel.pipelines-export.scheduled.is-enabled", Boolean::class.java, false)

  private val exportMinAge : Duration
    get() = springEnv.getProperty("keel.pipelines-export.scheduled.min-age-duration", Duration::class.java, Duration.ofDays(1))

  private val exportBatchSize : Int
    get() = springEnv.getProperty("keel.pipelines-export.scheduled.batch-size", Int::class.java, 5)

  /**
   * Converts a [cloudProvider] and resource [type] passed to an export function to a [ResourceKind].
   */
  private fun typeToKind(cloudProvider: String, type: String): ResourceKind =
    when(type) {
      "classicloadbalancer" -> EC2_CLASSIC_LOAD_BALANCER_V1.kind
      "classicloadbalancers" -> EC2_CLASSIC_LOAD_BALANCER_V1.kind
      "applicationloadbalancer" -> EC2_APPLICATION_LOAD_BALANCER_V1_2.kind
      "applicationloadbalancers" -> EC2_APPLICATION_LOAD_BALANCER_V1_2.kind
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
    val moniker = (resource.spec as? Monikered)?.let {
      it.moniker
    } ?: throw IllegalArgumentException("We can only export resources with a moniker currently.")

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
    if (!isScheduledExportEnabled) return
    val apps = deliveryConfigRepository.getAppsToExport(exportMinAge, exportBatchSize)
    log.debug("Running the migration export on apps: $apps")
    apps.forEach { app ->
      runBlocking {
        when (val result = exportFromPipelines(app)) {
          is ExportErrorResult -> deliveryConfigRepository.storeFailedPipelinesExportResult(app, result.error)
          is ExportSkippedResult -> log.info("Skipping export of $app - it is already on MD")
          is PipelineExportResult -> deliveryConfigRepository.storePipelinesExportResult(
            result.deliveryConfig,
            result.toSkippedPipelines(),
            result.exportSucceeded,
            result.repoSlug,
            result.projectKey
          )
        }
        updateApplicationScmStatus(app)
      }
    }
  }

  suspend fun updateApplicationScmStatus(applicationName: String) {
    val application = front50Cache.applicationByName(applicationName)
    val repoProjectKey = application.repoProjectKey
    val repoSlug = application.repoSlug
    if (repoProjectKey.isNullOrEmpty() || repoSlug.isNullOrEmpty()) {
      log.info("Application $applicationName is missing scm details")
      return
    }
    val isScmPowered = jobService.hasJobs(projectKey = repoProjectKey, repoSlug = repoSlug, type = application.repoType, scmType = scmConfig.jobType)
    deliveryConfigRepository.updateMigratingAppScmStatus(applicationName, isScmPowered)
  }

  /**
   * Given an application name, look up all the associated pipelines and attempt to build a delivery config
   * that represents the corresponding environments, artifacts and delivery flow.
   *
   * Supports only a sub-set of well-known pipeline patterns (see [EXPORTABLE_PIPELINE_SHAPES]).
   */
  suspend fun exportFromPipelines(
    applicationName: String,
    maxAgeDays: Long = 6L * 30L
  ): ExportResult {
    log.info("Exporting delivery config from pipelines for application $applicationName (max age: $maxAgeDays days)")

    val pipelines = front50Cache.pipelinesByApplication(applicationName)
    val application = front50Cache.applicationByName(applicationName)

    try {
      deliveryConfigRepository.getByApplication(applicationName)
      log.debug("found an existing config for application $applicationName. Will not process with export process.")
      return ExportSkippedResult(isManaged = true)
    } catch (e: NoDeliveryConfigForApplication) {
      log.debug("did not found an existing config for application $applicationName. Trying to export..")
    }

    val serviceAccount = application.email ?: DEFAULT_SERVICE_ACCOUNT

    //1. find pipelines which are not matching to the supported patterns above
    val nonExportablePipelines = pipelines.findNonExportable(maxAgeDays)
    //2. get the actual supported pipelines
    val exportablePipelines = pipelines - nonExportablePipelines.keys

    // this returns 1:1 mapping between a "deploy" stage and a server group associated
    val pipelinesToDeploysAndClusters = exportablePipelines.toDeploysAndClusters(serviceAccount)

    val environments = mutableSetOf<SubmittedEnvironment>()
    val artifacts = mutableSetOf<DeliveryArtifact>()
    var configValidationExecption: Exception? = null

    //3. iterate over the deploy pipelines, and map resources to the environments they represent
    val pipelinesToEnvironments = pipelinesToDeploysAndClusters.mapValues { (pipeline, deploysAndClusters) ->
      //4. iterate over the clusters, export their resources, create constraints and environment
      deploysAndClusters.mapNotNull { (deploy, cluster) ->
        log.debug("Attempting to build environment for cluster ${cluster.moniker}")
        val environmentName = figureOutEnvironmentName(cluster)
        val manualJudgement = if (pipeline.hasManualJudgment(deploy)) {
          log.debug("Adding manual judgment constraint for environment $environmentName based on manual judgment stage in pipeline ${pipeline.name}")
          setOf(ManualJudgementConstraint())
        } else {
          emptySet()
        }

        val allowedTimes = createAllowedTimeConstraint(deploy.restrictedExecutionWindow)

        //5. infer from the cluster all dependent resources, including artifacts
        val resourcesAndArtifacts = createResourcesBasedOnCluster(cluster, environments, applicationName)

        if (resourcesAndArtifacts.resources.isEmpty()) {
          log.debug("could not create resources from cluster ${cluster.moniker.toName()}")
          return@mapNotNull null
        }

        val environment = SubmittedEnvironment(
          name = environmentName,
          resources = resourcesAndArtifacts.resources,
          constraints = manualJudgement + allowedTimes,
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

    // Now look at the pipeline triggers and dependencies between pipelines and add constraints
    val finalEnvironments = pipelinesToEnvironments.flatMap { (pipeline, envs) ->
      envs.map { environment ->
        val constraints = triggersToEnvironmentConstraints(applicationName, pipeline, environment, pipelinesToEnvironments)
        environment.copy(
          constraints = environment.constraints + constraints,
          notifications = notifications(environment.name, application.slackChannel?.name)
        ).addMetadata(
          "exportedFrom" to pipeline.link(baseUrlConfig.baseUrl)
        )
      }
    }.toSet()
      .dedupe()

    val finalArtifacts = artifacts.dedupe()

    val deliveryConfig = SubmittedDeliveryConfig(
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
      log.debug("Delivery config for application ${application.name} didn't passed validation; caught exception", ex)
      configValidationExecption = ex
    }

    val result = PipelineExportResult(
      deliveryConfig = deliveryConfig,
      exported = pipelinesToEnvironments,
      skipped = nonExportablePipelines,
      baseUrl = baseUrlConfig.baseUrl,
      configValidationException = configValidationExecption?.message,
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
      setOf(TimeWindowConstraint(
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
   * Adding slack notifications for production environment, using the slack channel as defined in spinnaker
  */
  private fun notifications(envName: String, slackChannel: String?): Set<NotificationConfig> {
    if (envName == PRODUCTION_EVN && slackChannel != null) {
      return setOf(NotificationConfig(
        address = slackChannel,
        type = NotificationType.slack,
        frequency = NotificationFrequency.normal
      ))
    }
    return emptySet()
  }

  private fun figureOutEnvironmentName(cluster: Exportable): String {
    return with(cluster) {
      when {
        !moniker.stack.isNullOrEmpty() -> moniker.stack!!
        "test" in account -> TESTING_EVN
        "prod" in account -> PRODUCTION_EVN
        else -> account
      }
    }
  }


  /**
   * Finds non-exportable pipelines in the list and associates them with the reason why they're not.
   */
  private fun List<Pipeline>.findNonExportable(maxAgeDays: Long): Map<Pipeline, SkipReason> {
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
        pipeline.shape !in EXPORTABLE_PIPELINE_SHAPES -> SkipReason.SHAPE_NOT_SUPPORTED
        else -> null
      }
    }.filterNotNullValues()
  }

  //Take a cluster exportable and return a pair with its dependent resources specs (cluster, security groups, and load balancers, and the artifact).
  private suspend fun createResourcesBasedOnCluster(cluster: Exportable, environments: Set<SubmittedEnvironment>, applicationName: String
  ): Pair<Set<SubmittedResource<ResourceSpec>>, DeliveryArtifact?> {
    val spec = try {
      handlers.supporting(cluster.clusterKind).export(cluster)
    } catch (e: Exception) {
      log.error("Unable to export cluster ${cluster.moniker}: $e. Ignoring.")
      return Pair(emptySet(), null)
    }

    // Export the artifact matching the cluster while we're at it
    val artifact = handlers.supporting(cluster.clusterKind).exportArtifact(cluster)
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

  private suspend fun dependentResources(dependencies: Dependent,
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
        if (sg.locations.account == account && it.spec.displayName == name) {
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
        if (clb.locations.account == account && it.spec.displayName == name) {
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
        if (alb.locations.account == account && it.spec.displayName == name) {
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
  private fun triggersToEnvironmentConstraints(
    application: String,
    pipeline: Pipeline,
    environment: SubmittedEnvironment,
    pipelinesToEnvironments: Map<Pipeline, List<SubmittedEnvironment>>
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
            application == trigger.application
            pipeline.id == trigger.pipeline
          }
            ?.let { (_, envs) ->
              // use the last environment within the matching pipeline (which would match the last deploy,
              // in case there's more than one)
              envs.last()
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
  private fun Set<SubmittedEnvironment>.dedupe(): Set<SubmittedEnvironment> {
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
}

interface ExportResult

data class ExportSkippedResult(
  val isManaged: Boolean
) : ExportResult

data class ExportErrorResult(
  val error: String,
  val detail: Any? = null
) : Exception(error, detail as? Throwable), ExportResult

data class PipelineExportResult(
  val deliveryConfig: SubmittedDeliveryConfig,
  val configValidationException: String?,
  @JsonIgnore
  val exported: Map<Pipeline, List<SubmittedEnvironment>>,
  @JsonIgnore
  val skipped: Map<Pipeline, SkipReason>,
  @JsonIgnore
  val baseUrl: String,
  @JsonIgnore
  val repoSlug: String?,
  @JsonIgnore
  val projectKey: String?
) : ExportResult {
  companion object {
    val VALID_SKIP_REASONS = listOf(SkipReason.DISABLED, SkipReason.NOT_EXECUTED_RECENTLY)
  }

  val pipelines: Map<String, Any> = mapOf(
    "exported" to exported.entries.map { (pipeline, environments) ->
      mapOf(
        "name" to pipeline.name,
        "link" to pipeline.link(baseUrl),
        "shape" to pipeline.shape.joinToString(" -> "),
        "environments" to environments.map { it.name }
      )
    },
    "skipped" to skipped.map { (pipeline, reason) ->
      mapOf(
        "name" to pipeline.name,
        "link" to pipeline.link(baseUrl),
        "shape" to pipeline.shape.joinToString(" -> "),
        "reason" to reason
      )
    }
  )

  val exportSucceeded: Boolean
    get() = skipped.filterValues { !(VALID_SKIP_REASONS.contains(it)) }.isNullOrEmpty()
}

private val Exportable.clusterKind: ResourceKind
  get() = ExportService.PROVIDERS_TO_CLUSTER_KINDS[this.cloudProvider]
    ?: error("Unsupported cluster cloud provider '${this.cloudProvider}'")

private val Pair<Set<SubmittedResource<ResourceSpec>>, DeliveryArtifact?>.artifact: DeliveryArtifact?
  get() = this.second

private val Pair<Set<SubmittedResource<ResourceSpec>>, DeliveryArtifact?>.resources: Set<SubmittedResource<ResourceSpec>>
  get() = this.first

fun PipelineExportResult.toSkippedPipelines(): List<SkippedPipeline> =
  skipped.map { (pipeline, reason) ->
    SkippedPipeline(
      pipeline.id,
      pipeline.name,
      pipeline.link(baseUrl),
      pipeline.shape.joinToString(" -> "),
      reason
    )
  }

private fun Pipeline.link(baseUrl: String) = "$baseUrl/#/applications/${application}/executions/configure/${id}"

private fun Int.fromDayNumberToWeekday() :String =
  when (this) {
     1 -> DayOfWeek.SUNDAY
     2 -> DayOfWeek.MONDAY
     3 -> DayOfWeek.TUESDAY
     4 -> DayOfWeek.WEDNESDAY
     5 -> DayOfWeek.THURSDAY
     6 -> DayOfWeek.FRIDAY
    else -> DayOfWeek.SATURDAY
  }.toString()
