package com.netflix.spinnaker.keel.actuation

import com.fasterxml.jackson.annotation.JsonAlias
import com.netflix.buoy.sdk.model.RolloutTarget
import com.netflix.spinnaker.keel.api.TaskStatus
import java.time.Instant

/**
 * Provides the data needed by the UI to visualize a task
 */
interface ExecutionSummaryService {

  fun getSummary(executionId: String): ExecutionSummary?
}

data class ExecutionSummary(
  val name: String,
  val id: String,
  val status: TaskStatus,
  val currentStage: Stage?, // null if finished
  val stages: List<Stage>,
  val deployTargets: List<RolloutTargetWithStatus>,
  val error: String? = null,
  val rolloutWorkflowId: String? = null // non-null if [deployTargets] is not empty
)

// simplified representation of a stage from orca,
// we can add more detail as needed
data class Stage(
  val id: String,
  val type: String,
  val name: String,
  val status: TaskStatus,
  @JsonAlias("startTime")
  val _startTime: Long? = null,
  @JsonAlias("endTime")
  val _endTime: Long? = null,
  val refId: String? = null, //this is a short code for the stage, used in ordering
  val requisiteStageRefIds: List<String> = emptyList(), //this is a coded form of what stage goes after another stage/belongs to a stage
  val syntheticStageOwner: String? = null, // only appears with a before stage/after stage
  val context: Map<String, Any?> = emptyMap(),
  val outputs: Map<String, Any?> = emptyMap(),
  val tasks: List<StageTask> = emptyList()
) {
  val startTime: Instant?
    get() = _startTime?.let { Instant.ofEpochMilli(it) }

  val endTime: Instant?
    get() = _endTime?.let { Instant.ofEpochMilli(it) }
}

data class StageTask(
  val id: String,
  val name: String,
  val implementingClass: String,
  val startTime: Instant?,
  val endTime: Instant?,
  val status: TaskStatus,
  val stageStart: Boolean,
  val stageEnd: Boolean
)

data class RolloutTargetWithStatus(
  val rolloutTarget: RolloutTarget,
  val status: RolloutStatus
)

enum class RolloutStatus {
  NOT_STARTED, RUNNING, SUCCEEDED, FAILED
}
