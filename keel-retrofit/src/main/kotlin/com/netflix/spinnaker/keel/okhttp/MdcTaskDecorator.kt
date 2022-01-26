package com.netflix.spinnaker.keel.okhttp

import org.slf4j.MDC
import org.springframework.core.task.TaskDecorator

/**
 * A [TaskDecorator] that passes the [MDC] context from the starting thread down into the [Runnable]
 * tasks started from it.
 *
 * Based on https://moelholm.com/blog/2017/07/24/spring-43-using-a-taskdecorator-to-copy-mdc-data-to-async-threads
 */
class MdcTaskDecorator : TaskDecorator {
  override fun decorate(runnable: Runnable): Runnable {
    val contextMap = MDC.getCopyOfContextMap()
    return Runnable {
      try {
        MDC.setContextMap(contextMap)
        runnable.run()
      } finally {
        MDC.clear()
      }
    }
  }
}
