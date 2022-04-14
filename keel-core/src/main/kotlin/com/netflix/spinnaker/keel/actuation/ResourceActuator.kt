package com.netflix.spinnaker.keel.actuation

import brave.Tracer
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.ThreadPoolMonitor
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.ResourceDiff
import com.netflix.spinnaker.keel.api.ResourceDiffFactory
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.actuation.Task
import com.netflix.spinnaker.keel.api.plugins.ActionDecision
import com.netflix.spinnaker.keel.api.plugins.ResourceHandler
import com.netflix.spinnaker.keel.api.plugins.supporting
import com.netflix.spinnaker.keel.core.ResourceCurrentlyUnresolvable
import com.netflix.spinnaker.keel.enforcers.ActiveVerifications
import com.netflix.spinnaker.keel.enforcers.EnvironmentExclusionEnforcer
import com.netflix.spinnaker.keel.events.ResourceActuationLaunched
import com.netflix.spinnaker.keel.events.ResourceActuationVetoed
import com.netflix.spinnaker.keel.events.ResourceCheckError
import com.netflix.spinnaker.keel.events.ResourceCheckUnresolvable
import com.netflix.spinnaker.keel.events.ResourceDeltaDetected
import com.netflix.spinnaker.keel.events.ResourceDeltaResolved
import com.netflix.spinnaker.keel.events.ResourceDiffNotActionable
import com.netflix.spinnaker.keel.events.ResourceMissing
import com.netflix.spinnaker.keel.events.ResourceTaskFailed
import com.netflix.spinnaker.keel.events.ResourceTaskSucceeded
import com.netflix.spinnaker.keel.events.ResourceValid
import com.netflix.spinnaker.keel.events.VerificationBlockedActuation
import com.netflix.spinnaker.keel.exceptions.EnvironmentCurrentlyBeingActedOn
import com.netflix.spinnaker.keel.logging.withTracingContext
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.persistence.DeliveryConfigRepository
import com.netflix.spinnaker.keel.persistence.DiffFingerprintRepository
import com.netflix.spinnaker.keel.persistence.EnvironmentDeletionRepository
import com.netflix.spinnaker.keel.persistence.ResourceRepository
import com.netflix.spinnaker.keel.plugin.CannotResolveCurrentState
import com.netflix.spinnaker.keel.plugin.CannotResolveDesiredState
import com.netflix.spinnaker.keel.plugin.ResourceResolutionException
import com.netflix.spinnaker.keel.telemetry.ResourceCheckSkipped
import com.netflix.spinnaker.keel.veto.VetoEnforcer
import com.netflix.spinnaker.kork.exceptions.SpinnakerException
import com.netflix.spinnaker.kork.exceptions.SystemException
import com.netflix.spinnaker.kork.exceptions.UserException
import com.netflix.spinnaker.kork.plugins.proxy.ExtensionInvocationProxy
import com.newrelic.api.agent.Trace
import kotlinx.coroutines.async
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.supervisorScope
import org.slf4j.LoggerFactory
import org.springframework.cloud.sleuth.annotation.NewSpan
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import java.lang.reflect.Proxy
import java.time.Clock
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor

/**
 * The core component in keel responsible for resource state monitoring and actuation.
 *
 * The [checkResource] method of this class is called periodically by [CheckScheduler]
 * to check on the current state of a specific [Resource] via the [ResourceHandler.current] method
 * of the corresponding [ResourceHandler] plugin, compare that with the desired state obtained
 * from [ResourceHandler.desired], and finally call the appropriate resource CRUD method on
 * the [ResourceHandler] if differences are detected.
 */
@Component
class ResourceActuator(
  private val resourceRepository: ResourceRepository,
  private val deliveryConfigRepository: DeliveryConfigRepository,
  private val diffFingerprintRepository: DiffFingerprintRepository,
  private val environmentDeletionRepository: EnvironmentDeletionRepository,
  private val handlers: List<ResourceHandler<*, *>>,
  private val actuationPauser: ActuationPauser,
  private val vetoEnforcer: VetoEnforcer,
  private val publisher: ApplicationEventPublisher,
  private val clock: Clock,
  private val environmentExclusionEnforcer: EnvironmentExclusionEnforcer,
  spectator: Registry,
  private val diffFactory: ResourceDiffFactory,
  private val tracer: Tracer? = null
) {
  companion object {
    val asyncExecutor: Executor = Executors.newCachedThreadPool()
  }

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  init {
    ThreadPoolMonitor.attach(spectator, asyncExecutor as ThreadPoolExecutor, "keel-resource-actuator-thread-pool")
  }

  @Trace(dispatcher=true)
  @NewSpan
  suspend fun <T : ResourceSpec> checkResource(resource: Resource<T>) {
    withTracingContext(resource, tracer) {
      val id = resource.id
      try {
        val plugin = handlers.supporting(resource.kind)
        val deliveryConfig = deliveryConfigRepository.deliveryConfigFor(resource.id)
        val environment = checkNotNull(deliveryConfig.environmentFor(resource)) {
          "Failed to find environment for ${resource.id} in deliveryConfig ${deliveryConfig.name}"
        }

        log.debug("Checking resource $id in environment ${environment.name} of application ${deliveryConfig.application}")

        if (actuationPauser.isPaused(resource)) {
          log.debug("Actuation for resource {} is paused, skipping checks", id)
          publisher.publishEvent(ResourceCheckSkipped(resource.kind, id, "ActuationPaused"))
          return@withTracingContext
        }

        if (plugin.actuationInProgress(resource)) {
          log.debug("Actuation for resource {} is already running, skipping checks", id)
          publisher.publishEvent(ResourceCheckSkipped(resource.kind, id, "ActuationInProgress"))
          return@withTracingContext
        }

        if (deliveryConfig.isPromotionCheckStale()) {
          log.debug("Artifact promotion check for application {} is stale, skipping resource checks", deliveryConfig.application)
          publisher.publishEvent(ResourceCheckSkipped(resource.kind, id, "PromotionCheckStale"))
          return@withTracingContext
        }

        if (environmentDeletionRepository.isMarkedForDeletion(environment)) {
          log.debug("Skipping resource ${resource.id} as the parent environment is marked for deletion")
          return@withTracingContext
        }

        val (desired, current) = plugin.resolve(resource)
        val diff = diffFactory.compare(desired, current)

        if (diff.hasChanges()) {
          log.debug("Storing diff fingerprint for resource {} delta: {}", id, diff.toDebug())
          diffFingerprintRepository.store(id, diff)
        } else {
          val numActionsTaken = diffFingerprintRepository.actionTakenCount(id)
          log.debug("Clearing diff fingerprint for resource $id (current == desired) - we took $numActionsTaken action(s) to resolve the diff")
          diffFingerprintRepository.clear(id)

          log.info("Resource {} is valid", id)
          when (resourceRepository.lastEvent(id)) {
            is ResourceActuationLaunched -> log.debug("waiting for actuating task to be completed") // do nothing and wait
            is ResourceDeltaDetected, is ResourceTaskSucceeded, is ResourceTaskFailed -> {
              // if a delta was detected and a task wasn't launched, the delta is resolved
              // if a task was launched and it completed, either successfully or not, the delta is resolved
              publisher.publishEvent(ResourceDeltaResolved(resource, clock))
            }
            else -> publisher.publishEvent(ResourceValid(resource, clock))
          }
          return@withTracingContext
        }

        val vetoResponse = vetoEnforcer.canActuate(resource)
        if (!vetoResponse.allowed) {
          log.debug("Skipping actuation for resource {} because it was vetoed: {}", resource.id, vetoResponse.message)
          publisher.publishEvent(
            ResourceActuationVetoed(
              kind = resource.kind,
              id = resource.id,
              version = resource.version,
              application = resource.application,
              reason = vetoResponse.message,
              veto = vetoResponse.vetoName,
              suggestedStatus = vetoResponse.suggestedStatus,
              delta = diff.toConciseDeltaJson(),
              timestamp = clock.instant()
            )
          )
          return@withTracingContext
        }

        val decision = plugin.willTakeAction(resource, diff)
        if (decision.willAct) {
          when {
            current == null || current.isEmpty() -> {
              log.warn("Resource {} is missing", id)
              publisher.publishEvent(ResourceMissing(resource, clock))

              plugin.create(resource, diff)
                .also { tasks ->
                  publisher.publishEvent(ResourceActuationLaunched(resource, plugin.name, tasks, clock))
                  diffFingerprintRepository.markActionTaken(id)
                }
            }
            else -> {
              log.warn("Resource {} is invalid", id)
              log.info("Resource {} delta: {}", id, diff.toDebug())
              publisher.publishEvent(ResourceDeltaDetected(resource, diff.toDeltaJson(), clock))

              environmentExclusionEnforcer.withActuationLease(deliveryConfig, environment) {
                plugin.update(resource, diff)
                  .also { tasks ->
                    if (tasks.isNotEmpty()) {
                      publisher.publishEvent(ResourceActuationLaunched(resource, plugin.name, tasks, clock))
                      diffFingerprintRepository.markActionTaken(id)
                    }
                  }
              }
            }
          }
        } else {
          log.warn("Resource {} skipped because it can't be fixed: {} (diff: {})", id, decision.message, diff.toDeltaJson())
          if (diff.hasChanges()) {
            publisher.publishEvent(ResourceDeltaDetected(resource, diff.toDeltaJson(), clock))
          }
          publisher.publishEvent(ResourceDiffNotActionable(resource, decision.message))
        }
      } catch (e: ResourceCurrentlyUnresolvable) {
        log.warn("Resource check for {} failed (hopefully temporarily) due to {}", id, e.message)
        publisher.publishEvent(ResourceCheckUnresolvable(resource, e, clock))
      } catch (e: ActiveVerifications) {
        log.info("Resource {} can't be actuated because a verification is running", id, e.message)
        publisher.publishEvent(VerificationBlockedActuation(resource, e, clock))
      } catch (e: EnvironmentCurrentlyBeingActedOn) {
        log.info("Resource {} can't be actuated on because something else is happening in the environment", id, e.message)
      } catch (e: Exception) {
        log.error("Resource check for $id failed", e)
        publisher.publishEvent(ResourceCheckError(resource, e.toSpinnakerException(), clock))
      }
    }
  }

  private fun Any.isEmpty() =
    when(this) {
      is Map<*, *> -> this.isEmpty()
      is Collection<*> -> this.isEmpty()
      else -> false
    }

  private fun DeliveryConfig.isPromotionCheckStale(): Boolean {
    val age = Duration.between(
      deliveryConfigRepository.deliveryConfigLastChecked(this),
      clock.instant()
    )
    return age > Duration.ofMinutes(5)
  }

  private fun Exception.toSpinnakerException(): SpinnakerException =
    when (this) {
      is ResourceResolutionException -> when (cause) {
        is UserException, is SystemException -> cause
        else -> this
      }
      is UserException, is SystemException -> this
      else -> SystemException(this)
    } as SpinnakerException

  @NewSpan
  private suspend fun <T : Any> ResourceHandler<*, T>.resolve(resource: Resource<ResourceSpec>): Pair<T, T?> {
    return supervisorScope {
      if (isKotlin) {
        val desiredAsync = async {
          runCatching {
            desired(resource)
          }
        }
        val currentAsync = async {
          runCatching {
            current(resource)
          }
        }
        val desiredResult = desiredAsync.await()
        val currentResult = currentAsync.await()

        val desired = desiredResult.getOrElse {
          when (it) {
            is ResourceCurrentlyUnresolvable -> throw it
            else -> throw CannotResolveDesiredState(resource.id, it)
          }
        }
        val current = currentResult.getOrElse { throw CannotResolveCurrentState(resource.id, it) }

        desired to current
      } else {
        // for Java compatibility
        // TODO: handle catching errors from desired and current
        val desiredAsync = desiredAsync(resource, asyncExecutor).asDeferred()
        val currentAsync = currentAsync(resource, asyncExecutor).asDeferred()

        val desired = desiredAsync.await()
        val current = currentAsync.await()
        desired to current
      }
    }
  }

  private fun DeliveryConfig.environmentFor(resource: Resource<*>): Environment? =
    environments.firstOrNull {
      it.resources
        .map { r -> r.id }
        .contains(resource.id)
    }

  private val ResourceHandler<*, *>.isKotlin: Boolean
    get() = if (this is Proxy) {
        (Proxy.getInvocationHandler(this) as? ExtensionInvocationProxy)?.targetClass
      } else {
        this.javaClass
      }?.let {
        it.declaredAnnotations.any { annotation ->
          annotation.annotationClass.qualifiedName == "kotlin.Metadata"
        }
      }
      ?: true // err on the side of Kotlin

  // These extensions get round the fact tht we don't know the spec type of the resource from
  // the repository. I don't want the `ResourceHandler` interface to be untyped though.
  @Suppress("UNCHECKED_CAST")
  @NewSpan
  private suspend fun <S : ResourceSpec, R : Any> ResourceHandler<S, R>.desired(
    resource: Resource<*>
  ): R =
    desired(resource as Resource<S>)

  @Suppress("UNCHECKED_CAST")
  @NewSpan
  private suspend fun <S : ResourceSpec, R : Any> ResourceHandler<S, R>.desiredAsync(
    resource: Resource<*>, executor: Executor
  ): CompletableFuture<R> =
    desiredAsync(resource as Resource<S>, executor)

  @Suppress("UNCHECKED_CAST")
  @NewSpan
  private suspend fun <S : ResourceSpec, R : Any> ResourceHandler<S, R>.current(
    resource: Resource<*>
  ): R? =
    current(resource as Resource<S>)

  @Suppress("UNCHECKED_CAST")
  @NewSpan
  private suspend fun <S : ResourceSpec, R : Any> ResourceHandler<S, R>.currentAsync(
    resource: Resource<*>, executor: Executor
  ): CompletableFuture<R?> =
    currentAsync(resource as Resource<S>, executor)

  @Suppress("UNCHECKED_CAST")
  @NewSpan
  private suspend fun <S : ResourceSpec, R : Any> ResourceHandler<S, R>.willTakeAction(
    resource: Resource<*>,
    resourceDiff: ResourceDiff<*>
  ): ActionDecision =
    willTakeAction(resource as Resource<S>, resourceDiff as ResourceDiff<R>)

  @Suppress("UNCHECKED_CAST")
  @NewSpan
  private suspend fun <S : ResourceSpec, R : Any> ResourceHandler<S, R>.create(
    resource: Resource<*>,
    resourceDiff: ResourceDiff<*>
  ): List<Task> =
    create(resource as Resource<S>, resourceDiff as ResourceDiff<R>)

  @Suppress("UNCHECKED_CAST")
  @NewSpan
  private suspend fun <S : ResourceSpec, R : Any> ResourceHandler<S, R>.update(
    resource: Resource<*>,
    resourceDiff: ResourceDiff<*>
  ): List<Task> =
    update(resource as Resource<S>, resourceDiff as ResourceDiff<R>)

  @Suppress("UNCHECKED_CAST")
  @NewSpan
  private suspend fun <S : ResourceSpec> ResourceHandler<S, *>.actuationInProgress(
    resource: Resource<*>
  ): Boolean =
    actuationInProgress(resource as Resource<S>)
  // end type coercing extensions
}
