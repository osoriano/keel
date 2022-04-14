package com.netflix.spinnaker.keel.events

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.time.Clock
import java.time.Instant

enum class EventLevel {
  SUCCESS, INFO, WARNING, ERROR
}

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  property = "type",
  include = JsonTypeInfo.As.PROPERTY
)
@JsonSubTypes(
  JsonSubTypes.Type(value = ApplicationEvent::class),
  JsonSubTypes.Type(value = ResourceEvent::class)
)
abstract class PersistentEvent {
  abstract val scope: EventScope
  abstract val application: String
  abstract val ref: String // The unique ID of the thing associated with the scope. Defined in sub-classes.
  abstract val timestamp: Instant
  abstract val triggeredBy: String?
  abstract val displayText: String
  open val level: EventLevel = EventLevel.INFO

  companion object {
    val clock: Clock = Clock.systemUTC()
  }

  enum class EventScope {
    @JsonProperty("resource") RESOURCE,
    @JsonProperty("application") APPLICATION
  }
}

/**
 * Interface for persistent events that should not be repeated in history, in which case we store the
 * first time the event was triggered and the count of occurrences.
 */
interface NonRepeatableEvent {
  val firstTriggeredAt: Instant
  val count: Int
}

/**
 * Common interface implemented by all [ResourceEvent]s and certain [ApplicationEvent]s that affect all the
 * application's resources, such as pausing and resuming.
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  property = "type",
  include = JsonTypeInfo.As.PROPERTY
)
@JsonSubTypes(
  JsonSubTypes.Type(value = ResourceEvent::class),
  JsonSubTypes.Type(value = ApplicationActuationPaused::class),
  JsonSubTypes.Type(value = ApplicationActuationResumed::class)
)
interface ResourceHistoryEvent {
  val scope: PersistentEvent.EventScope
  val ref: String // the resource ID or application name
  val timestamp: Instant
}
