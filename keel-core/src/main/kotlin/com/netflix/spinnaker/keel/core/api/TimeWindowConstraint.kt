package com.netflix.spinnaker.keel.core.api

import com.netflix.spinnaker.keel.api.StatefulConstraint
import java.time.DateTimeException
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.zone.ZoneRulesException

const val OLD_ALLOWED_TIMES_CONSTRAINT_TYPE = "allowed-times-old"

/**
 * A constraint that requires the current time to fall within an allowed window
 */
//todo eb: remove
data class TimeWindowConstraint(
  val windows: List<TimeWindow>,
  val tz: String? = null,
  val maxDeploysPerWindow: Int? = null
) : StatefulConstraint(OLD_ALLOWED_TIMES_CONSTRAINT_TYPE) {
  init {
    if (tz != null) {
      val zoneId: ZoneId? = try {
        ZoneId.of(tz)
      } catch (e: Exception) {
        when (e) {
          is DateTimeException, is ZoneRulesException -> null
          else -> throw e
        }
      }

      require(zoneId != null) {
        "tz must be a valid java parseable ZoneId"
      }
    }
  }
}
val TimeWindowConstraint.zone: ZoneId?
  get() = tz?.let { ZoneId.of(it) }

/**
 * @return the window containing `time` or null if `time` is outside any window.
 */
fun TimeWindowConstraint.activeWindowOrNull(time: ZonedDateTime) =
  windowsNumeric.find { window ->
    val hoursMatch = window.hours.isEmpty() || window.hours.contains(time.hour)
    val daysMatch = window.days.isEmpty() || window.days.contains(time.dayOfWeek.value)
    daysMatch && hoursMatch
  }

fun TimeWindowConstraint.activeWindowBoundsOrNull(time: ZonedDateTime) : ZonedDateTimeRange? =
  activeWindowOrNull(time)?.windowRange(time)

val TimeWindowConstraint.windowsNumeric: List<TimeWindowNumeric>
  get() = windows.toNumeric()
