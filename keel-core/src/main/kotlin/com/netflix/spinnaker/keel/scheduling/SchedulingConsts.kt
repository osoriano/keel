package com.netflix.spinnaker.keel.scheduling

/**
 * For some reason, Gradle (but not IntelliJ) is having trouble resolving root-level const vals.
 */
object SchedulingConsts {

  // The temporal namespace that keel operates in
  const val TEMPORAL_NAMESPACE = "managed-delivery"

  // The base Temporal task queue name for resource scheduling.
  const val RESOURCE_SCHEDULER_TASK_QUEUE = "keel-resource-scheduler"

  // the tag for the temporal checker, used in atlas metrics
  const val TEMPORAL_CHECKER = "temporal"

  // A workflow memo field that tracks which environment a worker that started a particular workflow runs in;
// e.g. "laptop" or "prestaging".
  const val WORKER_ENV_SEARCH_ATTRIBUTE = "WorkerEnv"

  // The workflow ID for the singleton scheduler supervisor
  const val SCHEDULER_SUPERVISOR_WORKFLOW_ID = "keel-resource-scheduler-supervisor"
}
