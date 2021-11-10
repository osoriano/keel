package com.netflix.spinnaker.keel.export

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.spinnaker.config.BaseUrlConfig
import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.Dependency
import com.netflix.spinnaker.keel.api.DependencyType
import com.netflix.spinnaker.keel.api.Dependent
import com.netflix.spinnaker.keel.api.Exportable
import com.netflix.spinnaker.keel.api.ResourceKind
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.ec2.EC2_APPLICATION_LOAD_BALANCER_V1_2
import com.netflix.spinnaker.keel.api.ec2.EC2_CLUSTER_V1_1
import com.netflix.spinnaker.keel.api.ec2.EC2_SECURITY_GROUP_V1
import com.netflix.spinnaker.keel.api.ec2.LoadBalancerSpec
import com.netflix.spinnaker.keel.api.ec2.SecurityGroupSpec
import com.netflix.spinnaker.keel.api.plugins.ResourceHandler
import com.netflix.spinnaker.keel.api.plugins.supporting
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.core.api.DEFAULT_SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.core.api.DependsOnConstraint
import com.netflix.spinnaker.keel.core.api.ManualJudgementConstraint
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.core.api.SubmittedEnvironment
import com.netflix.spinnaker.keel.core.api.SubmittedResource
import com.netflix.spinnaker.keel.core.parseMoniker
import com.netflix.spinnaker.keel.filterNotNullValues
import com.netflix.spinnaker.keel.front50.Front50Cache
import com.netflix.spinnaker.keel.front50.model.DeployStage
import com.netflix.spinnaker.keel.front50.model.Pipeline
import com.netflix.spinnaker.keel.orca.ExecutionDetailResponse
import com.netflix.spinnaker.keel.orca.OrcaService
import com.netflix.spinnaker.keel.validators.DeliveryConfigValidator
import com.netflix.spinnaker.keel.veto.unhealthy.UnsupportedResourceTypeException
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
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
  private val validator: DeliveryConfigValidator
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

    if (application.managedDelivery?.importDeliveryConfig == true) {
      // This heuristic is not perfect - this value will exist for apps that were on MD and then removed. Good enough for now
      return ExportSkippedResult(isManaged = true)
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

        //5. infer from the cluster all dependent resources, including artifacts
        val resourcesAndArtifacts = createResourcesBasedOnCluster(cluster, environments)

        if (resourcesAndArtifacts.resources.isEmpty()) {
          log.debug("could not create resources from cluster ${cluster.moniker.toName()}")
          return@mapNotNull null
        }

        val environment = SubmittedEnvironment(
          name = environmentName,
          resources = resourcesAndArtifacts.resources,
          constraints = manualJudgement,
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
          constraints = environment.constraints + constraints
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
      environments = finalEnvironments.sensibleOrder()
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
      configValidationException = configValidationExecption?.message
    )

    log.info("Successfully exported delivery config:\n${prettyPrinter.writeValueAsString(result)}")
    return result
  }

  private fun figureOutEnvironmentName(cluster: Exportable): String {
    return with(cluster) {
      when {
        !moniker.stack.isNullOrEmpty() -> moniker.stack!!
        "test" in account -> "testing"
        "prod" in account -> "production"
        else -> account
      }
    }
  }


  /**
   * Finds non-exportable pipelines in the list and associates them with the reason why they're not.
   */
  private fun List<Pipeline>.findNonExportable(maxAgeDays: Long): Map<Pipeline, String> {
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
        pipeline.disabled -> "pipeline disabled"
        pipeline.fromTemplate -> "pipeline is from template"
        pipeline.hasParallelStages -> "pipeline has parallel stages"
        pipeline.shape !in EXPORTABLE_PIPELINE_SHAPES -> "pipeline doesn't match supported flows"
        (lastExecution == null || lastExecution.olderThan(maxAgeDays)) -> "pipeline hasn't run in the last $maxAgeDays days"
        else -> null
      }
    }.filterNotNullValues()
  }

  //Take a cluster exportable and return a pair with its dependent resources specs (cluster, security groups, and load balancers, and the artifact).
  private suspend fun createResourcesBasedOnCluster(cluster: Exportable, environments: Set<SubmittedEnvironment>
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
    val securityGroupResources = dependentResources(dependencies, environments, cluster, EC2_SECURITY_GROUP_V1.kind)
    val loadBalancersResources = dependentResources(dependencies, environments, cluster, EC2_APPLICATION_LOAD_BALANCER_V1_2.kind)

    return Pair(
      // combine dependent resources and cluster resource
      loadBalancersResources + securityGroupResources +
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
                                         kind: ResourceKind
  ): Set<SubmittedResource<ResourceSpec>> {

    val dependencyType = when (kind) {
      EC2_SECURITY_GROUP_V1.kind -> DependencyType.SECURITY_GROUP
      EC2_APPLICATION_LOAD_BALANCER_V1_2.kind -> DependencyType.LOAD_BALANCER
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
      // Check if we already exported any security group or alb when creating previous environments
      if ((kind == EC2_SECURITY_GROUP_V1.kind && checkIfExistsSecurityGroups(account, it.name, environments)) ||
        (kind == EC2_APPLICATION_LOAD_BALANCER_V1_2.kind && checkIfExistsLoadBalancer(account, it.name, environments))) {
        log.debug("found an existing resource with name ${it.name}, will not recreate it.")
      } else {
        try {
          val resourceSpec = handlers.supporting(kind).export(
            //copy everything from the cluster and just change the type
            cluster.copy(
              kind = kind,
              moniker = parseMoniker(it.name),
              account = account
            )
          )
          resourcesSpec.add(resourceSpec)
        } catch (ex: Exception) {
          log.debug("could not export ${it.name}", ex)
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

  private fun checkIfExistsSecurityGroups(account: String, name: String, environments: Set<SubmittedEnvironment>): Boolean {
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

  private fun checkIfExistsLoadBalancer(account: String, name: String, environments: Set<SubmittedEnvironment>): Boolean {
    environments.map { environment ->
      environment.resources.filter {
        it.spec is LoadBalancerSpec
      }.map {
        val alb = it.spec as LoadBalancerSpec
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
  val skipped: Map<Pipeline, String>,
  @JsonIgnore
  val baseUrl: String
) : ExportResult {
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
}

private val Exportable.clusterKind: ResourceKind
  get() = ExportService.PROVIDERS_TO_CLUSTER_KINDS[this.cloudProvider]
    ?: error("Unsupported cluster cloud provider '${this.cloudProvider}'")

private val Pair<Set<SubmittedResource<ResourceSpec>>, DeliveryArtifact?>.artifact: DeliveryArtifact?
  get() = this.second

private val Pair<Set<SubmittedResource<ResourceSpec>>, DeliveryArtifact?>.resources: Set<SubmittedResource<ResourceSpec>>
  get() = this.first

private fun Pipeline.link(baseUrl: String) = "$baseUrl/#/applications/${application}/executions/configure/${id}"
