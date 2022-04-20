package com.netflix.spinnaker.keel.scheduling

// The namespace for keel functionality
const val TEMPORAL_NAMESPACE = "managed-delivery"

// The base Temporal task queue name for resource scheduling.
const val RESOURCE_SCHEDULER_TASK_QUEUE = "keel-resource-scheduler"

// the tag for the temporal checker, used in atals metrics
const val TEMPORAL_CHECKER = "temporal"
