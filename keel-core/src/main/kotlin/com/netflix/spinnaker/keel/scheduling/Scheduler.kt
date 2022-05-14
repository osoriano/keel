package com.netflix.spinnaker.keel.scheduling

import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowMethod

interface Scheduler<T> {

  @WorkflowMethod
  fun schedule(request: T)

  @SignalMethod
  fun checkNow()
}
