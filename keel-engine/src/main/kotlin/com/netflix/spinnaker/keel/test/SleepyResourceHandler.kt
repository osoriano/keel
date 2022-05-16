package com.netflix.spinnaker.keel.test

import com.netflix.spinnaker.keel.api.Exportable
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.ResourceDiff
import com.netflix.spinnaker.keel.api.ResourceKind
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.ResourceStatus
import com.netflix.spinnaker.keel.api.ResourceStatus.HAPPY
import com.netflix.spinnaker.keel.api.actuation.Task
import com.netflix.spinnaker.keel.api.plugins.ActionDecision
import com.netflix.spinnaker.keel.api.plugins.ResourceHandler
import com.netflix.spinnaker.keel.api.plugins.SupportedKind
import com.netflix.spinnaker.keel.test.SleepyResourceHandler.SleepyResourceSpec
import kotlinx.coroutines.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import kotlin.random.Random

/**
 * Simple [ResourceHandler] whose only feature is to be able to sleep before returning results to each
 * suspending function based on configuration. Intended for usage with integration/load tests only. 
 *
 * To configure delays, set a value in the resource spec's `delays` field named after the applicable 
 * [ResourceHandler] method. For example, the configuration below would make the handler sleep 1 second in
 * the call to [current] and 200 milliseconds in the call to [desired]:
 * 
 * ```yaml
 * kind: test/sleepy@v1
 * metadata:
 *   name: sleepy
 * spec:
 *   delays:
 *     current: 1000
 *     desired: 200
 * ```
 */
@Component
class SleepyResourceHandler : ResourceHandler<SleepyResourceSpec, ResourceStatus> {
  companion object {
    val SLEEPY_RESOURCE_KIND = ResourceKind("test", "sleepy", "1")
    private val RESOURCE_STATUSES = ResourceStatus.values()
  }

  private val log: Logger = LoggerFactory.getLogger(SleepyResourceHandler::class.java)

  override val supportedKind: SupportedKind<SleepyResourceSpec>
    get() = SupportedKind(SLEEPY_RESOURCE_KIND, SleepyResourceSpec::class.java)

  suspend fun sleep(millis: Long) {
    try {
      val caller = Thread.currentThread().stackTrace[4].methodName // 2nd method down the stack + suspendImpl counterparts
      log.debug("Sleeping for {} millis in $caller...", millis)
      delay(millis)
      log.debug("Done sleeping in $caller.")
    } catch (e: InterruptedException) {
      log.debug(e.message, e)
    }
  }

  override suspend fun desired(resource: Resource<SleepyResourceSpec>): ResourceStatus {
    sleep(resource.spec.delays.desired)
    return HAPPY
  }

  override suspend fun current(resource: Resource<SleepyResourceSpec>): ResourceStatus {
    sleep(resource.spec.delays.current)
    return Random.nextInt(RESOURCE_STATUSES.size).let { RESOURCE_STATUSES[it] }
  }

  override suspend fun willTakeAction(
    resource: Resource<SleepyResourceSpec>,
    resourceDiff: ResourceDiff<ResourceStatus>
  ): ActionDecision {
    sleep(resource.spec.delays.willTakeAction)
    return ActionDecision(true)
  }

  override suspend fun upsert(resource: Resource<SleepyResourceSpec>, resourceDiff: ResourceDiff<ResourceStatus>): List<Task> {
    sleep(resource.spec.delays.upsert)
    return emptyList()
  }

  override suspend fun delete(resource: Resource<SleepyResourceSpec>): List<Task> {
    sleep(resource.spec.delays.delete)
    return emptyList()
  }

  override suspend fun actuationInProgress(resource: Resource<SleepyResourceSpec>): Boolean {
    sleep(resource.spec.delays.actuationInProgress)
    return false
  }

  override suspend fun export(exportable: Exportable): SleepyResourceSpec {
    return super.export(exportable)
  }

  data class SleepyResourceSpec(
    val delays: ResourceHandlerDelays
  ) : ResourceSpec
  
  data class ResourceHandlerDelays(
    val current: Long = 0L,
    val desired: Long = 0L,
    val willTakeAction: Long = 0L,
    val upsert: Long = 0L,
    val delete: Long = 0L,
    val actuationInProgress: Long = 0L
  )
}