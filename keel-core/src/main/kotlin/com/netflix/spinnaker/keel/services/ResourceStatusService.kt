package com.netflix.spinnaker.keel.services

import com.netflix.spinnaker.keel.api.actuation.Task
import com.netflix.spinnaker.keel.events.ApplicationActuationPaused
import com.netflix.spinnaker.keel.events.ApplicationActuationResumed
import com.netflix.spinnaker.keel.events.MaxResourceDeletionAttemptsReached
import com.netflix.spinnaker.keel.events.ResourceActuationLaunched
import com.netflix.spinnaker.keel.events.ResourceActuationResumed
import com.netflix.spinnaker.keel.events.ResourceActuationVetoed
import com.netflix.spinnaker.keel.events.ResourceCheckError
import com.netflix.spinnaker.keel.events.ResourceCheckUnresolvable
import com.netflix.spinnaker.keel.events.ResourceCreated
import com.netflix.spinnaker.keel.events.ResourceDeletionLaunched
import com.netflix.spinnaker.keel.events.ResourceDeltaDetected
import com.netflix.spinnaker.keel.events.ResourceDeltaResolved
import com.netflix.spinnaker.keel.events.ResourceDiffNotActionable
import com.netflix.spinnaker.keel.events.ResourceEvent
import com.netflix.spinnaker.keel.events.ResourceHistoryEvent
import com.netflix.spinnaker.keel.events.ResourceMissing
import com.netflix.spinnaker.keel.events.ResourceTaskFailed
import com.netflix.spinnaker.keel.events.ResourceTaskSucceeded
import com.netflix.spinnaker.keel.events.ResourceUpdated
import com.netflix.spinnaker.keel.events.ResourceValid
import com.netflix.spinnaker.keel.events.VerificationBlockedActuation
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.persistence.ResourceRepository
import com.netflix.spinnaker.keel.api.ResourceStatus
import com.netflix.spinnaker.keel.api.ResourceStatus.ACTUATING
import com.netflix.spinnaker.keel.api.ResourceStatus.CREATED
import com.netflix.spinnaker.keel.api.ResourceStatus.CURRENTLY_UNRESOLVABLE
import com.netflix.spinnaker.keel.api.ResourceStatus.DELETING
import com.netflix.spinnaker.keel.api.ResourceStatus.DIFF
import com.netflix.spinnaker.keel.api.ResourceStatus.DIFF_NOT_ACTIONABLE
import com.netflix.spinnaker.keel.api.ResourceStatus.ERROR
import com.netflix.spinnaker.keel.api.ResourceStatus.HAPPY
import com.netflix.spinnaker.keel.api.ResourceStatus.MISSING_DEPENDENCY
import com.netflix.spinnaker.keel.api.ResourceStatus.PAUSED
import com.netflix.spinnaker.keel.api.ResourceStatus.RESUMED
import com.netflix.spinnaker.keel.api.ResourceStatus.UNHAPPY
import com.netflix.spinnaker.keel.api.ResourceStatus.UNKNOWN
import com.netflix.spinnaker.keel.api.ResourceStatus.WAITING
import com.netflix.spinnaker.keel.events.ApplicationEvent
import org.springframework.stereotype.Component
import kotlin.math.min

/**
 * Service object that offers high-level APIs around resource (event) history and status.
 */
@Component
class ResourceStatusService(
  private val resourceRepository: ResourceRepository,
  private val actuationPauser: ActuationPauser
) {

  /**
   * Returns the status of the specified resource by first checking whether or not it or the parent application are
   * paused, then looking into the last few events in the resource's history.
   */
  fun getStatus(id: String): ResourceStatus {
    // For the PAUSED status, we look at the `paused` table as opposed to events, since these records
    // persist even when a delivery config/resource (and associated events) have been deleted. We do
    // this so we don't inadvertently start actuating on a resource that had been previously paused,
    // without explicit action from the user to resume.
    if (actuationPauser.isPaused(id)) {
      return PAUSED
    }

    val history = resourceRepository.eventHistory(id, 10)

    return getStatusFromHistory(history)
  }

  /**
   * Calculates a resource's status based on the specified [history] of events.
   */
  fun getStatusFromHistory(history: List<ResourceHistoryEvent>): ResourceStatus {
    val filteredHistory = history.filterForStatus()

    // sanity check events in history
    filteredHistory.filterIsInstance<ResourceEvent>().groupBy { it.ref }
      .also {
        if (it.size > 1) error("Resource history contains events for multiple resources (${it.keys})")
      }

    filteredHistory.filterIsInstance<ApplicationEvent>().groupBy { it.ref }
      .also {
        if (it.size > 1) error("Resource history contains events for multiple applications (${it.keys})")
      }

    return when {
      filteredHistory.isEmpty() -> UNKNOWN // shouldn't happen, but is a safeguard since events are persisted asynchronously
      filteredHistory.isHappy() -> HAPPY
      filteredHistory.isMissingDependency() -> MISSING_DEPENDENCY
      filteredHistory.isUnhappy() -> UNHAPPY // order matters! must be after all other veto-related statuses
      filteredHistory.isDiff() -> DIFF
      filteredHistory.isActuating() -> ACTUATING
      filteredHistory.isDiffNotActionable() -> DIFF_NOT_ACTIONABLE
      filteredHistory.isError() -> ERROR
      filteredHistory.isCreated() -> CREATED
      filteredHistory.isResumed() -> RESUMED
      filteredHistory.isDeleting() -> DELETING
      filteredHistory.isWaiting() -> WAITING // must be before CURRENTLY_UNRESOLVABLE because it's a special case of that status
      filteredHistory.isCurrentlyUnresolvable() -> CURRENTLY_UNRESOLVABLE
      else -> UNKNOWN
    }
  }

  fun getActuationState(id: String): ResourceActuationState {
    // For the PAUSED status, we look at the `paused` table as opposed to events, since these records
    // persist even when a delivery config/resource (and associated events) have been deleted. We do
    // this so we don't inadvertently start actuating on a resource that had been previously paused,
    // without explicit action from the user to resume.
    if (actuationPauser.isPaused(id)) {
      return ResourceActuationState(id, ResourceStatusUserFriendly.NOT_MANAGED)
    }

    val history = resourceRepository.eventHistory(id, 10)
      .filterForStatus()

    return when {
      history.isHappy() -> ResourceStatusUserFriendly.UP_TO_DATE.toActuationState(id, history = history)
      history.isBlocked() -> ResourceStatusUserFriendly.WAITING.toActuationState(id, reason = "Resource is locked while verifications are running", history = history)
      history.isEmpty() -> ResourceStatusUserFriendly.PROCESSING.toActuationState(id, reason = "New resource will be created shortly", history = history)
      history.isDiff() -> ResourceStatusUserFriendly.PROCESSING.toActuationState(id, reason = "Resource does not match the config and will be updated soon", history = history)
      history.isActuating() -> ResourceStatusUserFriendly.PROCESSING.toActuationState(id, reason = "Resource is being updated", history = history)
      history.isCreated() -> ResourceStatusUserFriendly.PROCESSING.toActuationState(id, reason = "New resource will be created shortly", history = history)
      history.isResumed() -> ResourceStatusUserFriendly.PROCESSING.toActuationState(id, reason = "Resource management will resume shortly", history = history)
      history.isDeleting() ->  ResourceStatusUserFriendly.DELETING.toActuationState(id, reason = "Resource is being deleted", history = history)
      history.isMissingDependency() -> ResourceStatusUserFriendly.ERROR.toActuationState(id, reason = history.lastRelevantMessage(), history = history)
      history.isVetoed() -> ResourceStatusUserFriendly.ERROR.toActuationState(id, reason = "We failed to update the resource multiple times", history = history)
      history.isDiffNotActionable() -> ResourceStatusUserFriendly.ERROR.toActuationState(id, reason = "We are unable to update resource to match the config", history = history)
      history.isError() -> ResourceStatusUserFriendly.ERROR.toActuationState(id, reason = history.lastRelevantMessage(), history = history)
      history.isCurrentlyUnresolvable() -> ResourceStatusUserFriendly.ERROR.toActuationState(id, reason = "We are temporarily unable to check resource status", history = history)
      else -> ResourceStatusUserFriendly.ERROR.toActuationState(id, reason = "Unknown reason", history = history)
    }

  }

  /**
   * Filter out resource events that are not relevant to determine status.
   */
  private fun List<ResourceHistoryEvent>.filterForStatus() =
    filter { it !is ResourceUpdated }

  private fun List<ResourceHistoryEvent>.isHappy(): Boolean {
    return first() is ResourceValid || first() is ResourceDeltaResolved
  }

  private fun List<ResourceHistoryEvent>.isBlocked(): Boolean {
    return first() is VerificationBlockedActuation
  }

  private fun List<ResourceHistoryEvent>.isDiffNotActionable(): Boolean {
    return first() is ResourceDiffNotActionable
  }

  private fun List<ResourceHistoryEvent>.isActuating(): Boolean {
    val first = first()
    val second = getOrNull(1)
    return first is ResourceActuationLaunched ||
      ( // TODO: we might want to move ResourceTaskFailed to isError later on
        (first is ResourceTaskSucceeded || first is ResourceTaskFailed)
          // the resource is only actuating if it's not being deleted
          && second !is ResourceDeletionLaunched
      )
  }

  private fun List<ResourceHistoryEvent>.isError(): Boolean {
    val first = first()
    return first is ResourceCheckError || first is MaxResourceDeletionAttemptsReached
  }

  private fun List<ResourceHistoryEvent>.isCreated(): Boolean {
    return first() is ResourceCreated
  }

  private fun List<ResourceHistoryEvent>.isWaiting(): Boolean {
    // we expect to have only two events (after we scrub paused/resumed events),
    // but we will accept several different "unresolvable" events
    // in order to be less brittle and show the user this status instead of an error
    val filtered = filterNot { it is ApplicationActuationPaused || it is ApplicationActuationResumed }
    return filtered.size < 5 && filtered.all { it is ResourceCreated || it is ResourceCheckUnresolvable }
  }

  private fun List<ResourceHistoryEvent>.isDiff(): Boolean {
    return first() is ResourceDeltaDetected || first() is ResourceMissing
  }

  private fun List<ResourceHistoryEvent>.isResumed(): Boolean {
    return first() is ResourceActuationResumed || first() is ApplicationActuationResumed
  }

  private fun List<ResourceHistoryEvent>.isCurrentlyUnresolvable(): Boolean {
    return first() is ResourceCheckUnresolvable
  }

  private fun List<ResourceHistoryEvent>.isVetoed(): Boolean {
    return first() is ResourceActuationVetoed && (first() as ResourceActuationVetoed).getStatus() == UNHAPPY
  }

  private fun List<ResourceHistoryEvent>.isDeleting(): Boolean {
    val first = first()
    return first is ResourceDeletionLaunched
  }

  /**
   * Returns true if a resource has been vetoed by the unhappy veto,
   * or if the last 10 events are only ResourceActuationLaunched or ResourceDeltaDetected events,
   * or if the resource has been vetoed by an unspecified veto that we don't have an explicit status mapping for.
   */
  private fun List<ResourceHistoryEvent>.isUnhappy(): Boolean {
    if (isVetoed()) {
      return true
    }

    val recentSliceOfHistory = this.subList(0, min(10, this.size))
    val filteredHistory = recentSliceOfHistory.filter { it is ResourceDeltaDetected || it is ResourceActuationLaunched }
    if (filteredHistory.size == recentSliceOfHistory.size) {
      return true
    }
    return false
  }

  /**
   * Determines if last event was a veto because of a missing dependency
   */
  private fun List<ResourceHistoryEvent>.isMissingDependency(): Boolean =
    first() is ResourceActuationVetoed && (first() as ResourceActuationVetoed).getStatus() == MISSING_DEPENDENCY

  /**
   * Determines the correct status to show for veto events
   */
  private fun ResourceActuationVetoed.getStatus(): ResourceStatus =
    when {
      // new style veto, gives us the status the resource should be
      suggestedStatus != null -> suggestedStatus
      // we can determine missing dependency by parsing the message
      isMissingDependency() -> MISSING_DEPENDENCY
      // all vetos get unhappy status if not specified
      else -> UNHAPPY
    }

  /**
   * Looks at the veto event and determines if it was vetoed by any of the [Required*Veto]s, which indicate a
   * missing dependency. Parses this information from the [reason]. This is used for backwards compatibility.
   */
  private fun ResourceActuationVetoed.isMissingDependency(): Boolean =
    reason?.contains("is not found in", true) ?: false
}

data class ResourceActuationState(
  val resourceId: String,
  val status: ResourceStatusUserFriendly,
  /** A user friendly reason based on our understanding of the current state */
  val reason: String? = null,
  /** The content of the last event */
  val eventMessage: String? = null,
  /** list of tasks associated with the event if any */
  val tasks: List<Task>? = emptyList(),
  /** list of errors associated with the event if any */
  val errors: List<String>? = emptyList()
)

fun List<ResourceHistoryEvent>.lastRelevantEvent(): ResourceEvent? =
  filterIsInstance<ResourceEvent>().firstOrNull()

fun List<ResourceHistoryEvent>.lastRelevantMessage(): String? =
  lastRelevantEvent()?.message

fun List<ResourceHistoryEvent>.lastEventTasks(): List<Task>? {
  return when (val first = firstOrNull()) {
    is ResourceTaskSucceeded -> first.tasks
    is ResourceTaskFailed -> first.tasks
    is ResourceActuationLaunched -> first.tasks
    else -> null
  }
}

enum class ResourceStatusUserFriendly {
  PROCESSING,
  UP_TO_DATE,
  ERROR,
  WAITING,
  NOT_MANAGED,
  DELETING;

  fun toActuationState(resourceId: String, reason: String? = null, history: List<ResourceHistoryEvent>): ResourceActuationState {
    val event = history.lastRelevantEvent()
    return ResourceActuationState(
      resourceId = resourceId,
      status = this,
      reason = reason,
      eventMessage = event?.message,
      tasks = history.lastEventTasks(),
      errors = (event as? ResourceCheckError)?.errors
    )
  }
}
