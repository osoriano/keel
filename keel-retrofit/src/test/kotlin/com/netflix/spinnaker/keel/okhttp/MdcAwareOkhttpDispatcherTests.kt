package com.netflix.spinnaker.keel.okhttp

import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.util.concurrent.Callable
import kotlin.random.Random

internal class MdcAwareOkhttpDispatcherTests {
  private val subject = MdcAwareOkhttpClientProvider(
    clientProviders = emptyList(),
    threadPoolCoreSize = 5,
    threadPoolMaxSize = 5
  ).dispatcher.executorService

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  @Test
  fun `passes MDC context from starting thread to thread pool runnables`() {
    val tasks = ('a'..'z').associateWith { key ->
      MDC.put("from-mdc", "value-$key")
      subject.submit(
        Callable {
          Thread.sleep(Random.nextLong(50))
          MDC.get("from-mdc").also {
            log.debug("Retrieved MDC value: $it")
          }
        }
      )
    }

    runBlocking {
      val jobs = tasks.mapValues { (key, task) ->
        launch { expectThat(task.get()).isEqualTo("value-$key") }
      }.values
      jobs.joinAll()
    }
  }
}
