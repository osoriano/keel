package com.netflix.spinnaker.keel.services

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

fun doInParallel(times: Int, block: () -> Unit) {
  GlobalScope.launch {
    repeat(times) {
      launch { block() }
    }
  }.apply {
    runBlocking {
      join()
    }
  }
}
