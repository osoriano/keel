package com.netflix.spinnaker.keel.caffeine

import com.github.benmanes.caffeine.cache.AsyncCacheLoader
import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import io.mockk.clearMocks
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.asExecutor
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import java.time.Duration

internal class CacheTests {
  private object CacheLoader {
    fun loadCache() =
      (1..5).associate { n -> "key$n" to "value$n" }

    fun loadValue(key: String) =
      "valueFor$key"
  }

  @Test
  @Disabled
  fun `map view returns all items if AsyncBulkLoadingCache manually reloaded`() {
    val cacheLoader = spyk(CacheLoader)
    val asyncBulkCache = TEST_CACHE_FACTORY.asyncBulkLoadingCache(
      cacheName = "test",
      defaultMaximumSize = 5,
      defaultExpireAfterWrite = Duration.ofSeconds(1),
      loader = cacheLoader::loadCache
    )
    val mapView = asyncBulkCache.asMap()

    runBlocking { asyncBulkCache.get("key1").await() } // trigger first load
    var values = runBlocking { mapView.values.map { it.await() } }
    verify(exactly = 1) { cacheLoader.loadCache() }
    expectThat(values).hasSize(5)

    // wait for cache to expire
    Thread.sleep(1100)

    clearMocks(cacheLoader)
    values = runBlocking { mapView.values.map { it.await() } }
    verify(exactly = 0) { cacheLoader.loadCache() }
    expectThat(values).hasSize(0)

    clearMocks(cacheLoader)
    runBlocking { asyncBulkCache.get("key1").await() } // manually trigger reload
    values = runBlocking { mapView.values.map { it.await() } }
    verify(exactly = 1) { cacheLoader.loadCache() }
    expectThat(values).hasSize(5)
  }

  @Test
  fun `map view retains items with refresh in AsyncLoadingCache`() {
    val cacheLoader = spyk(CacheLoader)
    val asyncLoadingCache = Caffeine.newBuilder()
      .executor(Dispatchers.IO.asExecutor())
      .maximumSize(5)
      .refreshAfterWrite(Duration.ofSeconds(1))
      .recordStats()
      .buildAsync(
        AsyncCacheLoader<String, String> { key, executor ->
          CoroutineScope(executor.asCoroutineDispatcher())
            .future { cacheLoader.loadValue(key) }
        }
      )

    runBlocking {
      repeat(5) { n -> asyncLoadingCache.get("key$n").await() }
    }
    verify(exactly = 5) { cacheLoader.loadValue(any()) }

    val mapView = asyncLoadingCache.asMap()
    expectThat(mapView).hasSize(5)

    // wait for cache to expire
    Thread.sleep(1100)

    val values = runBlocking { mapView.values.map { it.await() } }
    expectThat(values).hasSize(5)
  }

  @Test
  fun `map view is emptied after items expire in AsyncLoadingCache`() {
    val cacheLoader = spyk(CacheLoader)
    val asyncLoadingCache = Caffeine.newBuilder()
      .executor(Dispatchers.IO.asExecutor())
      .maximumSize(5)
      .expireAfterWrite(Duration.ofSeconds(1))
      .recordStats()
      .buildAsync(
        AsyncCacheLoader<String, String> { key, executor ->
          CoroutineScope(executor.asCoroutineDispatcher())
            .future { cacheLoader.loadValue(key) }
        }
      )

    runBlocking {
      repeat(5) { n -> asyncLoadingCache.get("key$n").await() }
    }
    verify(exactly = 5) { cacheLoader.loadValue(any()) }

    val mapView = asyncLoadingCache.asMap()
    expectThat(mapView).hasSize(5)

    // wait for cache to expire
    Thread.sleep(1100)

    val values = runBlocking { mapView.values.map { it.await() } }
    expectThat(values).isEmpty()
  }
}

