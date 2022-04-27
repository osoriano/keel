package com.netflix.spinnaker.keel.scheduling

// The namespace for keel functionality
const val TEMPORAL_NAMESPACE = "managed-delivery"

// The base Temporal task queue name for resource scheduling.
const val RESOURCE_SCHEDULER_TASK_QUEUE = "keel-resource-scheduler"

// the tag for the temporal checker, used in atlas metrics
const val TEMPORAL_CHECKER = "temporal"

// A workflow memo field that tracks which environment a worker that started a particular workflow runs in;
// e.g. "laptop" or "prestaging".
const val WORKFLOW_MEMO_WORKER_ENVIRONMENT = "WorkerEnvironment"
