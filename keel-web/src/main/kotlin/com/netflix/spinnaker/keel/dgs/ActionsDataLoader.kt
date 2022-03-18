package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsDataLoader
import com.netflix.graphql.dgs.context.DgsContext
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.action.ActionRepository
import com.netflix.spinnaker.keel.api.action.ActionStateFull
import com.netflix.spinnaker.keel.api.action.ActionType
import com.netflix.spinnaker.keel.api.action.EnvironmentArtifactAndVersion
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.graphql.types.MD_Action
import com.netflix.spinnaker.keel.graphql.types.MD_ActionStatus
import com.netflix.spinnaker.keel.graphql.types.MD_ActionType
import org.dataloader.BatchLoaderEnvironment
import org.dataloader.MappedBatchLoaderWithContext
import java.util.concurrent.CompletionStage
import org.springframework.context.ApplicationEventPublisher
import org.slf4j.LoggerFactory
import com.netflix.spinnaker.keel.telemetry.InvalidVerificationIdSeen
import com.netflix.springboot.scheduling.DefaultExecutor
import java.util.concurrent.Executor

/**
 * Loads all verification states for the given versions
 */
@DgsDataLoader(name = ActionsDataLoader.Descriptor.name)
class ActionsDataLoader(
  private val publisher: ApplicationEventPublisher,
  private val actionRepository: ActionRepository,
  @DefaultExecutor private val executor: Executor
) : MappedBatchLoaderWithContext<EnvironmentArtifactAndVersion, List<MD_Action>> {

  object Descriptor {
    const val name = "artifact-version-actions"
  }

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  fun loadData(
    config: DeliveryConfig,
    keys: MutableSet<EnvironmentArtifactAndVersion>
  ): MutableMap<EnvironmentArtifactAndVersion, List<MD_Action>> {
    val result = mutableMapOf<EnvironmentArtifactAndVersion, List<MD_Action>>()
    keys.groupBy { it.artifactReference }.entries.forEach { (artifactReference, entries) ->
      val allStates =
        actionRepository.getStatesForVersions(deliveryConfig = config, artifactReference = artifactReference,
                                              artifactVersions = entries.map { it.artifactVersion })

      allStates.entries.forEach { (key, states) ->
        result[key] = states.mapNotNull {
          it.toMdAction(
            ArtifactInEnvironmentContext(
              deliveryConfig = config,
              environmentName = key.environmentName,
              artifactReference = key.artifactReference,
              version = key.artifactVersion
            )
          )
        }
      }
    }
    return result
  }

  /**
   * Loads verifications and actions for each context
   */
  override fun load(keys: MutableSet<EnvironmentArtifactAndVersion>, environment: BatchLoaderEnvironment):
    CompletionStage<MutableMap<EnvironmentArtifactAndVersion, List<MD_Action>>> {
    val context: ApplicationContext = DgsContext.getCustomContext(environment)
    return executor.supplyAsync {
      val config = context.getConfig()
      loadData(config, keys)
    }
  }

  fun ActionStateFull.toMdAction(ctx: ArtifactInEnvironmentContext) =
    ctx.action(type, id)?.id?.let { actionId ->
      MD_Action(
        id = ctx.getMdActionId(type, id),
        type = actionId, // TODO: deprecated - remove after updating the frontend
        actionId = actionId,
        status = state.status.toDgsActionStatus(),
        startedAt = state.startedAt,
        completedAt = state.endedAt,
        link = state.link,
        actionType = MD_ActionType.valueOf(type.name)
      )
    }
      .also { if (ctx.action(type, id) == null) onInvalidVerificationId(id, ctx) }

  /**
   * Actions to take when the verification state database table references a verification id that doesn't exist
   * in the delivery config
   */
  fun onInvalidVerificationId(vId: String, ctx: ArtifactInEnvironmentContext) {
    publisher.publishEvent(
      InvalidVerificationIdSeen(
        vId,
        ctx.deliveryConfig.application,
        ctx.deliveryConfig.name,
        ctx.environmentName
      )
    )
    log.error("verification_state table contains invalid verification id: $vId  config: ${ctx.deliveryConfig.name} env: ${ctx.environmentName}. Valid ids in this env: ${ctx.environment.verifyWith.map { it.id }}")
  }
}

fun ConstraintStatus.toDgsActionStatus(): MD_ActionStatus = when (this) {
  ConstraintStatus.NOT_EVALUATED -> MD_ActionStatus.NOT_EVALUATED
  ConstraintStatus.PENDING -> MD_ActionStatus.PENDING
  ConstraintStatus.FAIL -> MD_ActionStatus.FAIL
  ConstraintStatus.PASS -> MD_ActionStatus.PASS
  ConstraintStatus.OVERRIDE_FAIL -> MD_ActionStatus.FAIL
  ConstraintStatus.OVERRIDE_PASS -> MD_ActionStatus.FORCE_PASS
  ConstraintStatus.SKIPPED -> MD_ActionStatus.SKIPPED
}

fun ArtifactInEnvironmentContext.getMdActionId(actionType: ActionType, actionId: String): String =
  "${deliveryConfig.application}-${environmentName}-${artifact.reference}-${version}-${actionType}-${actionId}"


