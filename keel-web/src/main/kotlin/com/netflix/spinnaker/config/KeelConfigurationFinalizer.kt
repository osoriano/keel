package com.netflix.spinnaker.config

import arrow.core.filterIsInstance
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.keel.actuation.ArtifactHandler
import com.netflix.spinnaker.keel.api.Alphabetical
import com.netflix.spinnaker.keel.api.ClusterDeployStrategy
import com.netflix.spinnaker.keel.api.Highlander
import com.netflix.spinnaker.keel.api.NoStrategy
import com.netflix.spinnaker.keel.api.OffStreamingPeak
import com.netflix.spinnaker.keel.api.RedBlack
import com.netflix.spinnaker.keel.api.ResourceDiffFactory
import com.netflix.spinnaker.keel.api.ResourceKind
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.RollingPush
import com.netflix.spinnaker.keel.api.RolloutStrategy
import com.netflix.spinnaker.keel.api.Staggered
import com.netflix.spinnaker.keel.api.constraints.DeploymentConstraintEvaluator
import com.netflix.spinnaker.keel.api.constraints.StatefulConstraintEvaluator
import com.netflix.spinnaker.keel.api.constraints.StatelessConstraintEvaluator
import com.netflix.spinnaker.keel.api.ec2.ApplicationLoadBalancerSpec.Action
import com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier
import com.netflix.spinnaker.keel.api.plugins.ConstraintEvaluator
import com.netflix.spinnaker.keel.api.plugins.PostDeployActionHandler
import com.netflix.spinnaker.keel.api.plugins.Resolver
import com.netflix.spinnaker.keel.api.plugins.ResourceHandler
import com.netflix.spinnaker.keel.api.plugins.SupportedKind
import com.netflix.spinnaker.keel.api.plugins.VerificationEvaluator
import com.netflix.spinnaker.keel.api.plugins.supporting
import com.netflix.spinnaker.keel.api.support.ExtensionRegistry
import com.netflix.spinnaker.keel.api.support.JvmExtensionType
import com.netflix.spinnaker.keel.api.support.extensionsOf
import com.netflix.spinnaker.keel.api.support.register
import com.netflix.spinnaker.keel.bakery.BaseImageCache
import com.netflix.spinnaker.keel.ec2.jackson.registerEc2Subtypes
import com.netflix.spinnaker.keel.ec2.jackson.registerKeelEc2ApiModule
import com.netflix.spinnaker.keel.k8s.KubernetesExtensionType
import com.netflix.spinnaker.keel.k8s.KubernetesResourceHandler
import com.netflix.spinnaker.keel.k8s.KubernetesResourceSpec
import com.netflix.spinnaker.keel.resources.SpecMigrator
import com.netflix.spinnaker.keel.titus.jackson.registerKeelTitusApiModule
import com.netflix.spinnaker.keel.titus.jackson.registerTitusSubtypes
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

/**
 * Component that wraps up keel configuration once all other beans have been instantiated.
 */
@Component
class KeelConfigurationFinalizer(
  private val baseImageCache: BaseImageCache? = null,
  private val resourceHandlers: List<ResourceHandler<*, *>> = emptyList(),
  private val specMigrators: List<SpecMigrator<*, *>> = emptyList(),
  private val constraintEvaluators: List<ConstraintEvaluator<*>> = emptyList(),
  private val verificationEvaluators: List<VerificationEvaluator<*>> = emptyList(),
  private val postDeployActionHandlers: List<PostDeployActionHandler<*>> = emptyList(),
  private val artifactHandlers: List<ArtifactHandler> = emptyList(),
  private val artifactSuppliers: List<ArtifactSupplier<*, *>> = emptyList(),
  private val objectMappers: List<ObjectMapper>,
  private val extensionRegistry: ExtensionRegistry,
  private val resolvers: List<Resolver<*>>,
  private val resourceDiffFactory: ResourceDiffFactory
) {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  @PostConstruct
  fun addDifferCustomizations() {
    val diffMixins = resourceHandlers.flatMap { it.getDiffMixins() }
    resourceDiffFactory.addMixins(diffMixins)
  }

  // TODO: not sure if we can do this more dynamically
  @PostConstruct
  fun registerApiExtensionsWithObjectMappers() {
    // Registering sub-types with the extension registry is redundant with the call to
    // registerKeelEc2ApiModule below, as far as object mappers go, but needed for the schema generator.
    extensionRegistry.registerEc2Subtypes()
    extensionRegistry.registerTitusSubtypes()
    objectMappers.forEach {
      it.registerKeelEc2ApiModule()
      it.registerKeelTitusApiModule()
    }
  }

  @PostConstruct
  fun registerResourceSpecSubtypes() {
    (resourceHandlers.map { it.supportedKind } + specMigrators.map { it.input })
      .forEach { (kind, specClass) ->
        if (specClass == KubernetesResourceSpec::class.java) {
          log.info("Registering CRD {} as ResourceSpec kind {}", null, kind)
          val handler = resourceHandlers.supporting(kind) as KubernetesResourceHandler
          extensionRegistry.register<ResourceSpec>(handler.extensionType, kind.toString())
        } else {
          log.info("Registering ResourceSpec sub-type {}: {}", kind, specClass.simpleName)
          extensionRegistry.register(specClass, kind.toString())
        }
      }
  }

  @PostConstruct
  fun registerConstraintSubtypes() {
    constraintEvaluators
      .map { it.supportedType }
      .forEach { constraintType ->
        log.info("Registering Constraint sub-type {}: {}", constraintType.name, constraintType.type.simpleName)
        extensionRegistry.register(constraintType.type, constraintType.name)
      }
  }

  @PostConstruct
  fun registerVerificationSubtypes() {
    verificationEvaluators
      .map { it.supportedVerification }
      .forEach { (type, implementingClass) ->
        log.info("Registering Verification sub-type {}: {}", type, implementingClass.simpleName)
        extensionRegistry.register(implementingClass, type)
      }
  }

  @PostConstruct
  fun registerPostDeploySubtypes() {
    postDeployActionHandlers
      .map { it.supportedType }
      .forEach { postDeployActionType ->
        log.info("Registering post deploy action runner sub-type {}: {}", postDeployActionType.name, postDeployActionType.type.simpleName)
        extensionRegistry.register(postDeployActionType.type, postDeployActionType.name)
      }
  }

  @PostConstruct
  fun resisterConstraintAttributeSubtypes() {
    constraintEvaluators
      .filterIsInstance<StatefulConstraintEvaluator<*, *>>()
      .map { it.attributeType }
      .forEach { attributeType ->
        log.info("Registering Constraint Attributes sub-type {}: {}", attributeType.name, attributeType.type.simpleName)
        extensionRegistry.register(attributeType.type, attributeType.name)
      }

    constraintEvaluators
      .filterIsInstance<StatelessConstraintEvaluator<*, *>>()
      .map { it.attributeType }
      .forEach { attributeType ->
        log.info("Registering Constraint Attributes sub-type {}: {}", attributeType.name, attributeType.type.simpleName)
        extensionRegistry.register(attributeType.type, attributeType.name)
      }

    constraintEvaluators
      .filterIsInstance<DeploymentConstraintEvaluator<*, *>>()
      .map { it.attributeType }
      .forEach { attributeType ->
        log.info("Registering Constraint Attributes sub-type {}: {}", attributeType.name, attributeType.type.simpleName)
        extensionRegistry.register(attributeType.type, attributeType.name)
      }
  }

  @PostConstruct
  fun registerArtifactSupplierSubtypes() {
    artifactSuppliers
      .map { it.supportedArtifact }
      .forEach { (name, artifactClass) ->
        log.info("Registering DeliveryArtifact sub-type {}: {}", name, artifactClass.simpleName)
        extensionRegistry.register(artifactClass, name)
      }
  }

  @PostConstruct
  fun registerClusterDeployStrategySubtypes() {
    extensionRegistry.register<ClusterDeployStrategy>(RedBlack::class.java, "red-black")
    extensionRegistry.register<ClusterDeployStrategy>(Highlander::class.java, "highlander")
    extensionRegistry.register<ClusterDeployStrategy>(NoStrategy::class.java, "none")
    extensionRegistry.register<ClusterDeployStrategy>(RollingPush::class.java, "rolling-push")
  }

  @PostConstruct
  fun registerRolloutStrategies() {
    extensionRegistry.register<RolloutStrategy>(Alphabetical::class.java, "alphabetical")
    extensionRegistry.register<RolloutStrategy>(OffStreamingPeak::class.java, "off-streaming-peak")
    extensionRegistry.register<RolloutStrategy>(Staggered::class.java, "staggered")
  }

  @PostConstruct
  fun registerListenerActionSubtypes() {
    with(extensionRegistry) {
      register<Action>(Action.RedirectAction::class.java, "redirect")
      register<Action>(Action.ForwardAction::class.java, "forward")
      register<Action>(Action.AuthenticateOidcAction::class.java, "authenticate-oidc")
    }
  }

  @PostConstruct
  fun initialStatus() {
    sequenceOf(
      BaseImageCache::class to baseImageCache?.javaClass
    )
      .forEach { (type, implementation) ->
        log.info("{} implementation: {}", type.simpleName, implementation?.simpleName)
      }

    val kinds = extensionRegistry
      .extensionsOf<ResourceSpec>()
      .filterIsInstance<String, JvmExtensionType>()
      .map { SupportedKind(ResourceKind.parseKind(it.key), it.value?.type as Class<out ResourceSpec>) }

    val crds = extensionRegistry
      .extensionsOf<ResourceSpec>()
      .filterIsInstance<String, KubernetesExtensionType>()
      .map { "${it.value.group}.${it.value.kind}" }

    log.info("Supported resources: {}", kinds.joinToString { it.kind.toString() })
    log.info("Supported Kubernetes resources: {}", crds.joinToString())
    log.info("Supported artifacts: {}", artifactSuppliers.joinToString { it.supportedArtifact.name })
    log.info("Using resource handlers: {}", resourceHandlers.joinToString { it.name })
    log.info("Using artifact handlers: {}", artifactHandlers.joinToString { it.name })
    log.info("Using verification evaluators: {}", verificationEvaluators.joinToString { it.javaClass.simpleName })
    log.info("Using post deploy action runners: {}", postDeployActionHandlers.joinToString { it.javaClass.simpleName })
    log.info("Using resolvers: {}", resolvers.joinToString { it.name })
  }
}
