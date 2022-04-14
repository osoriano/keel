package com.netflix.spinnaker.keel.events

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.netflix.spinnaker.keel.events.EventLevel.WARNING
import java.time.Clock
import java.time.Instant

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  property = "type",
  include = JsonTypeInfo.As.PROPERTY
)
@JsonSubTypes(
  JsonSubTypes.Type(value = ApplicationActuationPaused::class, name = "ApplicationActuationPaused"),
  JsonSubTypes.Type(value = ApplicationActuationResumed::class, name = "ApplicationActuationResumed")
)
abstract class ApplicationEvent(
  override val triggeredBy: String? = null
) : PersistentEvent() {
  override val scope = EventScope.APPLICATION

  override val ref: String
    get() = application
}

/**
 * Actuation at the application level has been paused.
 */
data class ApplicationActuationPaused(
  override val application: String,
  override val timestamp: Instant,
  override val triggeredBy: String?,
  val comment: String? = null,
  override val level: EventLevel = WARNING,
  override val displayText: String = "Application management paused",
  override val firstTriggeredAt: Instant = timestamp,
  override val count: Int = 1,
) : ApplicationEvent(), ResourceHistoryEvent, NonRepeatableEvent {

  constructor(application: String, triggeredBy: String, comment: String? = null, clock: Clock = Companion.clock) : this(
    application,
    clock.instant(),
    triggeredBy,
    comment
  )
}

/**
 * Actuation at the application level has resumed.
 */
data class ApplicationActuationResumed(
  override val application: String,
  override val triggeredBy: String?,
  override val timestamp: Instant,
  override val displayText: String = "Application management resumed",
  override val firstTriggeredAt: Instant = timestamp,
  override val count: Int = 1,
) : ApplicationEvent(), ResourceHistoryEvent, NonRepeatableEvent {

  constructor(application: String, triggeredBy: String, clock: Clock = Companion.clock) : this(
    application,
    triggeredBy,
    clock.instant()
  )
}
