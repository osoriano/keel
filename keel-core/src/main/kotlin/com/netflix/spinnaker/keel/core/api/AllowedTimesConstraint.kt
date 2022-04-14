package com.netflix.spinnaker.keel.core.api

import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.DeploymentConstraint
import com.netflix.spinnaker.keel.api.StatefulConstraint
import com.netflix.spinnaker.keel.exceptions.InvalidConstraintException
import java.text.ParsePosition
import java.time.DateTimeException
import java.time.DayOfWeek
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.TextStyle
import java.time.temporal.ChronoUnit.HOURS
import java.time.zone.ZoneRulesException
import java.util.Locale

const val ALLOWED_TIMES_CONSTRAINT_TYPE = "allowed-times"

/**
 * A constraint that requires the current time to fall within an allowed window
 */
data class AllowedTimesConstraint(
  val windows: List<TimeWindow>,
  val tz: String? = null,
  val maxDeploysPerWindow: Int? = null
) : DeploymentConstraint(ALLOWED_TIMES_CONSTRAINT_TYPE) {
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


val AllowedTimesConstraint.zone: ZoneId?
  get() = tz?.let { ZoneId.of(it) }

/**
 * @return the window containing `time` or null if `time` is outside any window.
 */
fun AllowedTimesConstraint.activeWindowOrNull(time: ZonedDateTime) =
  windowsNumeric.find { window ->
    val hoursMatch = window.hours.isEmpty() || window.hours.contains(time.hour)
    val daysMatch = window.days.isEmpty() || window.days.contains(time.dayOfWeek.value)
    daysMatch && hoursMatch
  }

fun AllowedTimesConstraint.activeWindowBoundsOrNull(time: ZonedDateTime) : ZonedDateTimeRange? =
  activeWindowOrNull(time)?.windowRange(time)

val AllowedTimesConstraint.windowsNumeric: List<TimeWindowNumeric>
  get() = windows.toNumeric()

/**
 * Numeric day and hour representation of a time window.
 * This format is consistent regardless of how the window is defined in a string.
 */
data class TimeWindowNumeric(
  val days: Set<Int>,
  val hours: Set<Int>,
)

/**
 * The start hour of a time window.
 */
val TimeWindowNumeric.startHour
  get() = hours.minOrNull() ?: 0

/**
 * The end hour of a time window.
 */
val TimeWindowNumeric.endHour
  get() = hours.maxOrNull() ?: 23

/**
 * @param time must be a date/time that is inside the window or result may not be valid.
 * @return the most recent time the window started.
 */
fun TimeWindowNumeric.windowRange(time: ZonedDateTime): ZonedDateTimeRange =
  ZonedDateTimeRange(
    time.withHour(startHour).truncatedTo(HOURS),
    time.withHour(endHour).truncatedTo(HOURS).plusHours(1).minusSeconds(1)
  )


data class TimeWindow(
  val days: String? = null,
  val hours: String? = null
) {
  init {
    if (hours != null) {
      require(validateHours(hours)) {
        "allowed-times hours must only contains hours 0-23 that are comma separated " +
          "and/or containing dashes to denote a range"
      }
    }

    if (days != null) {
      require(validateDays(days)) {
        "allowed-times days must only contain any of " +
          "${dayAliases + daysOfWeek} " +
          "that are comma separated and/or containing dashes to denote a range"
      }
    }
  }
}

val whiteSpace = """\s""".toRegex()
val intOnly = """^\d+$""".toRegex()
val intRange = """^\d+-\d+$""".toRegex()
val separators = """[\s,\-]""".toRegex()
val fullDayFormatter = DateTimeFormatter.ofPattern("EEEE", Locale.getDefault())
val shortDayFormatter = DateTimeFormatter.ofPattern("EEE", Locale.getDefault())

val daysOfWeek = DayOfWeek.values()
  .map {
    listOf(
      it.toString().lowercase(),
      it.getDisplayName(TextStyle.SHORT, Locale.getDefault()).lowercase()
    )
  }
  .flatten()
  .toSet()

val dayAliases = setOf("weekdays", "weekends")

fun validateHours(hours: String): Boolean {
  return hours.split(separators).all {
    it.matches(intOnly) && it.toInt() >= 0 && it.toInt() <= 23
  }
}

fun validateDays(days: String): Boolean {
  return days.lowercase().split(separators).all {
    daysOfWeek.contains(it) || dayAliases.contains(it)
  }
}

fun parseHours(hourConfig: String?): Set<Int> {
  if (hourConfig == null) {
    return emptySet()
  }

  val hours = mutableSetOf<Int>()
  val trimmed = hourConfig.replace(whiteSpace, "")
  val elements = trimmed.split(",")

  elements.forEach {
    when {
      it.isInt() -> hours.add(it.toInt())
      it.isIntRange() -> hours.addAll(it.hourRange())
    }
  }

  return hours
}

fun String.isInt(): Boolean = this.matches(intOnly)

fun String.isIntRange(): Boolean = this.matches(intRange)

fun String.hourRange(): Set<Int> {
  val hours = split("-").map { it.toInt() }
  return if (hours[1] > hours[0]) {
    // i.e. 10-18
    (hours[0] until hours[1]).toSet()
  } else {
    // i.e. 18-04 == between 18-23 || 0-4
    (hours[0]..23).toSet() + (0 until hours[1]).toSet()
  }
}

/**
 * Used for converting the day config into a list of numbers for the UI.
 */
fun parseDays(dayConfig: String?): Set<Int> {
  if (dayConfig == null) {
    return emptySet()
  }

  val days = mutableSetOf<DayOfWeek>()
  val trimmed = dayConfig.replace(whiteSpace, "")
    .lowercase()
  val elements = trimmed.split(",")

  elements.forEach {
    when {
      it.isDayAlias() -> days.addAll(it.dayAlias())
      it.isDay() -> days.add(it.toDayOfWeek())
      it.isDayRange() -> days.addAll(it.dayRange())
    }
  }

  return days.map(DayOfWeek::getValue).toSet()
}

fun String.isDay(): Boolean = daysOfWeek.contains(this)

fun String.isDayAlias(): Boolean = dayAliases.contains(this)

fun String.isDayRange(): Boolean {
  val days = this.split("-")
  if (days.size != 2) {
    return false
  }

  return daysOfWeek.contains(days[0]) && daysOfWeek.contains(days[1])
}

fun String.dayAlias(): Set<DayOfWeek> =
  when (this) {
    "weekdays" -> setOf(DayOfWeek.MONDAY, DayOfWeek.TUESDAY, DayOfWeek.WEDNESDAY, DayOfWeek.THURSDAY, DayOfWeek.FRIDAY)
    "weekends" -> setOf(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY)
    else -> throw InvalidConstraintException(ALLOWED_TIMES_CONSTRAINT_TYPE, "Failed parsing day alias $this")
  }

fun String.toDayOfWeek(): DayOfWeek =
  (fullDayFormatter.parseUnresolved(this.capitalize(), ParsePosition(0))
    ?: shortDayFormatter.parseUnresolved(this.capitalize(), ParsePosition(0)))
    ?.let { DayOfWeek.from(it) }
    ?: throw InvalidConstraintException(ALLOWED_TIMES_CONSTRAINT_TYPE, "Failed parsing day '$this'")

fun String.dayRange(): Set<DayOfWeek> {
  /**
   * Convert Mon-Fri or Monday-Friday to [DayOfWeek] integers to compute
   * a range, then back to a set of individual days that today can be
   * matched against.
   */
  val days = this.split("-").map { it.capitalize() }
  val day1 = days[0].toDayOfWeek()
  val day2 = days[1].toDayOfWeek()

  return if (day2 > day1) {
    // Mon - Fri
    (day1.value..day2.value).map { DayOfWeek.of(it) }
  } else {
    // Fri - Mon
    ((day1.value..7) + (1..day2.value)).map { DayOfWeek.of(it) }
  }
    .toSet()
}

fun List<TimeWindow>.toNumeric(): List<TimeWindowNumeric> =
  map {
    TimeWindowNumeric(
      days = parseDays(it.days),
      hours = parseHours(it.hours)
    )
  }

class ZonedDateTimeRange(
  override val start: ZonedDateTime,
  override val endInclusive: ZonedDateTime
) : ClosedRange<ZonedDateTime>
