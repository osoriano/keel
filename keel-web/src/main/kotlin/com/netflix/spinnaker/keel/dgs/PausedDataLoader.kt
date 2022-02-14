package com.netflix.spinnaker.keel.dgs

import com.netflix.graphql.dgs.DgsDataLoader
import com.netflix.spinnaker.keel.graphql.types.MD_PausedInfo
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.pause.PauseScope
import org.dataloader.BatchLoaderEnvironment
import org.dataloader.MappedBatchLoaderWithContext
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * Loads all paused info for resources
 */
@DgsDataLoader(name = PausedDataLoader.Descriptor.name)
class PausedDataLoader(
  val actuationPauser: ActuationPauser
) : MappedBatchLoaderWithContext<PausedKey, MD_PausedInfo> {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  object Descriptor {
    const val name = "paused"
  }

  override fun load(
    keys: MutableSet<PausedKey>,
    environment: BatchLoaderEnvironment?
  ): CompletionStage<MutableMap<PausedKey, MD_PausedInfo>> {
    return CompletableFuture.supplyAsync {
      load(keys)
    }
  }

  fun load(keys: MutableSet<PausedKey>): MutableMap<PausedKey, MD_PausedInfo> {
    val loadedData: MutableMap<PausedKey, MD_PausedInfo> = mutableMapOf()
    keys
      .groupBy { it.scope }
      .forEach { (scope, keys) ->
        // query each scope separately, but in bulk
        val pauses = actuationPauser.bulkPauseInfo(scope, keys.map { it.name })
        val loaded: MutableMap<PausedKey, MD_PausedInfo> = pauses.associate { pause ->
          // transform into the return type for this data loader
          PausedKey(scope, pause.name) to pause.toDgsPaused()
        }.toMutableMap()

        // store in our list of data
        loadedData.putAll(loaded)
      }
    return loadedData
  }
}

/**
 * Key for the paused data loader
 * name: either application name or resource id
 */
data class PausedKey(
  val scope: PauseScope,
  val name: String
)
