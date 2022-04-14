package com.netflix.spinnaker.keel.front50

import com.netflix.spinnaker.keel.caffeine.CacheFactory
import com.netflix.spinnaker.keel.caffeine.CacheProperties
import com.netflix.spinnaker.keel.exceptions.ApplicationNotFound
import com.netflix.spinnaker.keel.api.Application
import com.netflix.spinnaker.keel.api.GitRepository
import com.netflix.spinnaker.keel.api.ManagedDeliveryConfig
import com.netflix.spinnaker.keel.retrofit.RETROFIT_NOT_FOUND
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isEmpty
import strikt.assertions.isFailure
import strikt.assertions.isSuccess
import strikt.assertions.isTrue
import io.mockk.coEvery as every
import io.mockk.coVerify as verify

class Front50CacheTests {
  private val cacheFactory = CacheFactory(SimpleMeterRegistry(), CacheProperties())
  private val appsByName = (1..10).associate {
    "app-$it" to Application(
      name = "app-$it",
      email = "owner@keel.io",
      repoType = "stash",
      repoProjectKey = "spinnaker",
      repoSlug = "keel-$it"
    )
  }
  private val front50Service: Front50Service = mockk()
  private val subject = Front50Cache(front50Service, cacheFactory)

  @BeforeEach
  fun setupMocks() {
    every {
      front50Service.allApplications(any())
    } returns appsByName.values.take(9)

    every {
      front50Service.applicationByName(any(), any())
    } answers {
      appsByName[arg(0)] ?: throw RETROFIT_NOT_FOUND
    }

    every {
      front50Service.searchApplications(any(), any())
    } answers {
      val name = arg<Map<String, String>>(0).entries.first().value
      listOfNotNull(appsByName[name])
    }

    every {
      front50Service.updateApplication(any(), any(), any())
    } answers {
      val updatedApp = arg<Application>(2)
      appsByName[arg(0)]?.copy(managedDelivery = updatedApp.managedDelivery) ?: throw RETROFIT_NOT_FOUND
    }
  }

  @Test
  fun `uses the same call to prime both caches`() {
    subject.primeCaches()

    verify(exactly = 1) {
      front50Service.allApplications(any())
    }
  }

  @Test
  fun `an application cached during priming is not fetched again`() {
    subject.primeCaches()

    runBlocking {
      subject.applicationByName("app-1")
    }

    verify(exactly = 0) {
      front50Service.applicationByName("app-1")
    }
  }

  @Test
  fun `an application not cached during priming is fetched individually`() {
    subject.primeCaches()

    runBlocking {
      subject.applicationByName("app-10")
    }

    verify(exactly = 1) {
      front50Service.applicationByName("app-10")
    }
  }

  @Test
  fun `failure to retrieve app is bubbled up`() {
    expectCatching { subject.applicationByName("unknown-app") }
      .isFailure()
      .isA<ApplicationNotFound>()
  }

  @Test
  fun `an app is cached by search params`() {
    runBlocking {
      repeat(3) {
        subject.searchApplications(mapOf("name" to "app-1"))
      }
    }

    verify(exactly = 1) {
      front50Service.searchApplications(mapOf("name" to "app-1"))
    }
  }

  @Test
  fun `non-matching search params returns an empty list`() {
    expectCatching { subject.searchApplications(mapOf("name" to "no-match")) }
      .isSuccess()
      .isEmpty()
  }

  @Test
  fun `importing an application from Git updates the application by name cache`() {
    val app = appsByName.values.first()
    runBlocking {
      subject.applicationByName(app.name)
    }
    runBlocking {
      subject.updateManagedDeliveryConfig(app, "keel", ManagedDeliveryConfig(importDeliveryConfig = true))
    }
    val cachedApp = runBlocking {
      subject.applicationByName(app.name)
    }
    verify(exactly = 1) {
      front50Service.applicationByName(any())
    }

    expectThat(cachedApp.managedDelivery?.importDeliveryConfig).isTrue()
  }

  @Test
  fun `importing an application from Git clears the cache`() {
    val app = appsByName.values.first()
    runBlocking {
      subject.searchApplicationsByRepo(GitRepository(app.repoType!!, app.repoProjectKey!!, app.repoSlug!!))
    }
    runBlocking {
      subject.updateManagedDeliveryConfig(app, "keel", ManagedDeliveryConfig(importDeliveryConfig = true))
    }
    runBlocking {
      subject.searchApplicationsByRepo(GitRepository(app.repoType!!, app.repoProjectKey!!, app.repoSlug!!))
    }
    verify(exactly = 2) {
      front50Service.searchApplications(any())
    }
  }
}
