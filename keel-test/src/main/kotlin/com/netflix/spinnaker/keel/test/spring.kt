package com.netflix.spinnaker.keel.test

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.springframework.core.env.Environment


/**
 * Generate a mock Spring environment that returns the default value for all boolean properties
 */
fun mockEnvironment() : Environment {
  val defaultBooleanValue = slot<Boolean>()
  val defaultIntValue = slot<Int>()
  val environment: Environment = mockk()

  every {
    hint(Boolean::class)
    environment.getProperty(any(), Boolean::class.java, capture(defaultBooleanValue))
  } answers {
    defaultBooleanValue.captured
  }

  every {
    hint(Int::class)
    environment.getProperty(any(), Int::class.java, capture(defaultIntValue))
  } answers {
    defaultIntValue.captured
  }

  return environment
}
