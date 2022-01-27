package com.netflix.spinnaker.keel.coroutines

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext

fun <T> runWithIoContext(block: suspend () -> T): T {
  return runBlocking {
    withContext(Dispatchers.IO) {
      block()
    }
  }
}
