package com.netflix.spinnaker.keel.activation

import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A class that provides the up/down in discovery mechanism.
 *
 * Use this class if your component needs to do work only while an instance is up.
 */
open class DiscoveryActivated {
  val log by lazy { LoggerFactory.getLogger(javaClass) }

  val enabled = AtomicBoolean(false)

  @EventListener(ApplicationUp::class)
  fun onApplicationUp() {
    log.info("Application up, enabling work in ${this.javaClass.simpleName}")
    enabled.set(true)
  }

  @EventListener(ApplicationDown::class)
  fun onApplicationDown() {
    log.info("Application down, disabling work in ${this.javaClass.simpleName}")
    enabled.set(false)
  }
}
