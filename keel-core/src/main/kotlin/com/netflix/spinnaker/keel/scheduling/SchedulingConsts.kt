package com.netflix.spinnaker.keel.scheduling

/**
 * For some reason, Gradle (but not IntelliJ) is having trouble resolving root-level const vals.
 */
object SchedulingConsts {

  // The temporal namespace that keel operates in
  const val TEMPORAL_NAMESPACE = "managed-delivery"

  // The Temporal task queue name for resource scheduling.
  const val RESOURCE_SCHEDULER_TASK_QUEUE = "keel-resource-scheduler"

  // The Temporal task queue name for environment scheduling.
  const val ENVIRONMENT_SCHEDULER_TASK_QUEUE = "keel-environment-scheduler"

  // the tag for the temporal checker, used in atlas metrics
  const val TEMPORAL_CHECKER = "temporal"

  // A workflow memo field that tracks which environment a worker that started a particular workflow runs in;
// e.g. "laptop" or "prestaging".
  const val WORKER_ENV_SEARCH_ATTRIBUTE = "WorkerEnv"

  // The workflow ID for the singleton resource scheduler supervisor
  const val RESOURCE_SCHEDULER_SUPERVISOR_WORKFLOW_ID = "keel-resource-scheduler-supervisor"

  // The workflow ID for the singleton environment scheduler supervisor
  const val ENVIRONMENT_SCHEDULER_SUPERVISOR_WORKFLOW_ID = "keel-environment-scheduler-supervisor"
}
