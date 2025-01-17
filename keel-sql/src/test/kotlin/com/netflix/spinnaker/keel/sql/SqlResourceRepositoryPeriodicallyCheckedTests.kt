package com.netflix.spinnaker.keel.sql

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.plugins.UnsupportedKind
import com.netflix.spinnaker.keel.events.ResourceState
import com.netflix.spinnaker.keel.persistence.ResourceRepositoryPeriodicallyCheckedTests
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.test.TEST_API_V1
import com.netflix.spinnaker.keel.test.deliveryConfig
import com.netflix.spinnaker.keel.test.mockEnvironment
import com.netflix.spinnaker.keel.test.resource
import com.netflix.spinnaker.keel.test.resourceFactory
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil.cleanupDb
import dev.minutest.rootContext
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.first
import strikt.assertions.hasSize
import strikt.assertions.isA
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isSuccess
import java.time.Clock
import java.time.Clock.systemUTC
import java.time.Duration
import java.util.Collections.synchronizedSet

internal object SqlResourceRepositoryPeriodicallyCheckedTests :
  ResourceRepositoryPeriodicallyCheckedTests<SqlResourceRepository>() {

  private val jooq = testDatabase.context
  private val retryProperties = RetryProperties(1, 0)
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties))
  private val resourceFactory = resourceFactory()

  override val factory: (clock: Clock) -> SqlResourceRepository = { clock ->
    SqlResourceRepository(
      jooq,
      clock,
      configuredObjectMapper(),
      resourceFactory,
      sqlRetry,
      spectator = NoopRegistry(),
      springEnv = mockEnvironment()
    )
  }

  val deliveryConfigRepository = SqlDeliveryConfigRepository(jooq, systemUTC(), configuredObjectMapper(), resourceFactory, sqlRetry, spectator = NoopRegistry())

  override val storeDeliveryConfig: (DeliveryConfig) -> Unit = deliveryConfigRepository::store

  override fun deliveryConfigFor(resourceId: String): DeliveryConfig =
    deliveryConfigRepository.deliveryConfigFor(resourceId)

  override fun flush() {
    cleanupDb(jooq)
  }

  fun parallelCheckingTests() = rootContext<Fixture<Resource<ResourceSpec>, SqlResourceRepository>> {
    fixture {
      Fixture(factory, createAndStore, updateOne)
    }

    after { flush() }

    /**
     * I'd like to have this test in the superclass but the in-memory implementation is not designed
     * to be thread safe.
     */
    context("many threads are checking simultaneously") {


      before {
        val resources = (0..1000).mapTo(mutableSetOf()) { i ->
          val resource = resource(id = "fnord-$i")
          subject.store(resource)
        }
        storeDeliveryConfig(deliveryConfig(resources = resources))

      }

      test("each thread gets a unique set of resources") {
        // We need to enable retries to handle occasional aborted transactions
        val customSqlRetry = RetryProperties(2, 1).let {
          SqlRetry(SqlRetryProperties(it, it))
        }
        // create a new repository object that is configured with our custom retries
        val repo =
          SqlResourceRepository(jooq, clock, configuredObjectMapper(), resourceFactory,
            customSqlRetry, spectator = NoopRegistry(), springEnv = mockEnvironment())
        val results = synchronizedSet<Resource<ResourceSpec>>(HashSet())

        doInParallel(500) {
          repo.itemsDueForCheck(ifNotCheckedInLast, limit)
            .onEach { repo.markCheckComplete(it, ResourceState.Ok) }
            .let(results::addAll)
        }

        expectThat(results.size).describedAs("number of unique resources processed (${results.size})").isEqualTo(1000)
      }
    }
  }

  fun unreadableResourceTests() = rootContext<Fixture<Resource<ResourceSpec>, SqlResourceRepository>> {
    fixture {
      Fixture(
        factory,
        { count ->
          val unreadableResource = resource(
            kind = TEST_API_V1.qualify("unreadable"),
            id = "unreadable"
          ).also(subject::store)
          val readableResources = (2..count).mapTo(mutableSetOf()) { i ->
            resource(id = "readable-$i").also {
              subject.store(it)
            }
          }
          (setOf(unreadableResource) + readableResources).also { resources ->
            storeDeliveryConfig(deliveryConfig(resources = resources))
          }
        },
        updateOne
      )
    }

    before { createAndStore(4) }

    after { flush() }

    context("there's an unreadable resource in the database") {
      test("fetching next items throws an exception the first time it is called") {
        expectCatching { nextResults() }.isFailure().isA<UnsupportedKind>()
      }

      test("subsequent calls will start returning valid results") {
        expectCatching { nextResults() }.isFailure().isA<UnsupportedKind>()
        expectCatching { nextResults() }.isSuccess().hasSize(2)
        expectCatching { nextResults() }.isSuccess().hasSize(0)
      }

      test("after the last check time limit has passed we never try to read the unreadable resource again") {
        expectCatching { nextResults() }.isFailure().isA<UnsupportedKind>()
        expectCatching { nextResults() }.isSuccess().hasSize(2)
        expectCatching { nextResults() }.isSuccess().hasSize(0)
        clock.incrementBy(Duration.ofMinutes(31))
        expectCatching { nextResults() }.isSuccess().hasSize(2)
        expectCatching { nextResults() }.isSuccess().hasSize(1)
        expectCatching { nextResults() }.isSuccess().hasSize(0)
      }
    }
  }

  fun versionedResourceTests() = rootContext<Fixture<Resource<ResourceSpec>, SqlResourceRepository>> {
    fixture {
      Fixture(factory, createAndStore, updateOne)
    }

    context("multiple versions of a resource exist") {
      before {
        createAndStore(1)
        updateOne()
      }

      test("the older resource version is never checked") {
        expectThat(nextResults())
          .hasSize(1)
          .first()
          .get(Resource<*>::version) isEqualTo 2

        expectThat(nextResults())
          .isEmpty()
      }
    }
  }
}

private fun doInParallel(times: Int, block: () -> Unit) {
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
