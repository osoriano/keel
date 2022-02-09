@file:Suppress("INVISIBLE_REFERENCE", "INVISIBLE_MEMBER", "EXPOSED_PARAMETER_TYPE")
package kotlinx.coroutines.scheduling

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Tag
import com.netflix.spectator.api.patterns.PolledMeter
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.scheduling.CoroutineScheduler.WorkerState
import org.slf4j.LoggerFactory
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.jvmName

/**
 * Monitors and reports metrics for the default Kotlin coroutine scheduler.
 *
 * Note that this monitor relies on internal Kotlin APIs with no guarantee of backward compatibility
 * and may break with newer releases of the coroutines libraries. Works as of 1.16.0.
 *
 * Provided in the absence of https://github.com/Kotlin/kotlinx.coroutines/issues/1360
 */
internal object CoroutineDispatcherMonitor {
  private const val ID_TAG_NAME = "id"
  private const val DEFAULT_ID = "default"
  private const val WORKER_COUNT = "coroutineScheduler.workerCount"
  private const val MAX_POOL_SIZE = "coroutineScheduler.maxPoolSize"
  private const val CORE_POOL_SIZE = "coroutineScheduler.corePoolSize"
  private const val CPU_QUEUE_SIZE = "coroutineScheduler.cpuQueueSize"
  private const val BLOCKING_QUEUE_SIZE = "coroutineScheduler.blockingQueueSize"

  private val log = LoggerFactory.getLogger(CoroutineDispatcherMonitor::class.java)

  /**
   * Start monitoring the specified [dispatcher], optionally tagged with a name.
   *
   * @param registry The registry to use.
   * @param dispatcher The [CoroutineDispatcher] to monitor.
   * @param dispatcherName The name of the dispatcher (defaults to "default").
   */
  fun attach(
    registry: Registry,
    dispatcher: CoroutineDispatcher,
    dispatcherName: String? = null
  ) {
    val coroutineScheduler = dispatcher.coroutineScheduler()
      ?: run {
        log.warn("Failed to monitor CoroutineScheduler metrics for ${dispatcher::class.jvmName}")
        return
      }

    log.info("Monitoring CoroutineScheduler metrics for ${dispatcher::class.jvmName}")

    val idValue = if (dispatcherName == null || dispatcherName.isEmpty()) {
      DEFAULT_ID
    } else {
      dispatcherName
    }

    val idTag: Tag = BasicTag(ID_TAG_NAME, idValue)

    WorkerState.values().forEach { state ->
      PolledMeter.using(registry)
        .withName(WORKER_COUNT)
        .withTag(idTag)
        .withTag(BasicTag("state", state.name))
        .monitorMonotonicCounter(coroutineScheduler) {
          getWorkerCount(coroutineScheduler, state).toLong()
        }
    }

    PolledMeter.using(registry)
      .withName(MAX_POOL_SIZE)
      .withTag(idTag)
      .monitorValue(coroutineScheduler) {
        it.maxPoolSize.toDouble()
      }

    PolledMeter.using(registry)
      .withName(CORE_POOL_SIZE)
      .withTag(idTag)
      .monitorValue(coroutineScheduler) {
        it.corePoolSize.toDouble()
      }

    PolledMeter.using(registry)
      .withName(CPU_QUEUE_SIZE)
      .withTag(idTag)
      .monitorValue(coroutineScheduler) {
        it.globalCpuQueue.size.toDouble()
      }

    PolledMeter.using(registry)
      .withName(BLOCKING_QUEUE_SIZE)
      .withTag(idTag)
      .monitorValue(coroutineScheduler) {
        it.globalBlockingQueue.size.toDouble()
      }
  }

  private fun getWorkerCount(coroutineScheduler: CoroutineScheduler, state: WorkerState): Int {
    var count = 0
    // first slot is reserved, so we start at 1
    for (index in 1 until coroutineScheduler.workers.length()) {
      val worker = coroutineScheduler.workers[index] ?: continue
      // need to get this as a field otherwise the getter is used and returns a Thread.State instead of WorkerState
      val workerState = CoroutineScheduler.Worker::class.memberProperties.find { it.name == "state" }?.get(worker)
      if (workerState == state) {
        ++count
      }
    }
    return count
  }
}

internal fun CoroutineDispatcher.coroutineScheduler(): CoroutineScheduler? =
  (this as? SchedulerCoroutineDispatcher)?.let {
    it.executor as? CoroutineScheduler
  }
