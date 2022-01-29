package com.netflix.spinnaker.keel.telemetry

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Counter
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.histogram.PercentileTimer
import com.netflix.spinnaker.keel.telemetry.Util.Companion.logger
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant

private class Util {
  companion object {
    val logger: Logger by lazy { LoggerFactory.getLogger(Util::class.java) }
  }
}

fun Counter.safeIncrement() =
  try {
    increment()
  } catch (ex: Exception) {
    logger.error("Exception incrementing {} counter: {}", id().name(), ex.message)
  }

fun Registry.recordDurationPercentile(metricName: String, startTime: Instant, endTime: Instant, tags: Set<BasicTag> = emptySet()) =
  PercentileTimer
    .builder(this)
    .withName(metricName)
    .withTags(tags)
    .build()
    .record(Duration.between(startTime, endTime))

fun Registry.recordDuration(metricName: String, startTime: Instant, endTime: Instant, tags: Set<BasicTag> = emptySet()) {
  val duration = Duration.between(startTime, endTime)
  timer(metricName, tags).record(duration)
}

fun Registry.recordDuration(metricName: String, startTime: Instant, endTime: Instant, vararg tags: Pair<String, String>) =
  recordDuration(metricName, startTime, endTime, tags.map { (k, v) -> BasicTag(k, v) }.toSet())
