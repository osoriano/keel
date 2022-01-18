package com.netflix.spinnaker.keel.logging

import com.netflix.spinnaker.keel.api.Exportable
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.test.resource
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.MDC
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNull

internal class TracingSupportTests : JUnit5Minutests {
  val resource = resource()
  val exportable = Exportable(
    cloudProvider = "aws",
    account = "test",
    user = "fzlem@netflix.com",
    moniker = Moniker("keel"),
    regions = emptySet(),
    kind = resource.kind
  )
  val publishedArtifact = PublishedArtifact(
    name = "org/image",
    type = DOCKER,
    version = "master-h230.88b3f71"
  )

  fun tests() = rootContext {
    before {
      MDC.clear()
    }

    after {
      MDC.clear()
    }

    context("running with resource tracing context") {
      test("injects $X_MANAGED_DELIVERY_RESOURCE to MDC in the coroutine context from resource") {
        runBlocking {
          launch {
            withTracingContext(resource) {
              expectThat(MDC.get(X_MANAGED_DELIVERY_RESOURCE))
                .isEqualTo(resource.id)
            }
          }
        }
      }

      test("injects $X_MANAGED_DELIVERY_RESOURCE to MDC in the coroutine context from exportable") {
        runBlocking {
          launch {
            withTracingContext(exportable) {
              expectThat(MDC.get(X_MANAGED_DELIVERY_RESOURCE))
                .isEqualTo(exportable.toResourceId())
            }
          }
        }
      }

      test("removes $X_MANAGED_DELIVERY_RESOURCE from MDC after block executes") {
        runBlocking {
          MDC.put("foo", "bar")
          launch {
            withTracingContext(resource) {
              expectThat(MDC.get(X_MANAGED_DELIVERY_RESOURCE))
                .isEqualTo(resource.id)
            }
          }.join()
          expectThat(MDC.get(X_MANAGED_DELIVERY_RESOURCE))
            .isNull()
          expectThat(MDC.get("foo"))
            .isEqualTo("bar")
        }
      }

      test("does not mix up $X_MANAGED_DELIVERY_RESOURCE between parallel coroutines") {
        runBlocking {
          val coroutine1 = async {
            withTracingContext(resource) {
              println("Resource trace ID: ${MDC.get(X_MANAGED_DELIVERY_RESOURCE)}")
              expectThat(MDC.get(X_MANAGED_DELIVERY_RESOURCE))
                .isEqualTo(resource.id)
            }
          }
          val coroutine2 = async {
            val anotherResource = resource()
            withTracingContext(anotherResource) {
              println("Resource trace ID: ${MDC.get(X_MANAGED_DELIVERY_RESOURCE)}")
              expectThat(MDC.get(X_MANAGED_DELIVERY_RESOURCE))
                .isEqualTo(anotherResource.id)
            }
          }
          coroutine1.await()
          coroutine2.await()
        }
      }
    }

    context("running with artifact tracing context") {
      test("injects $X_MANAGED_DELIVERY_ARTIFACT to MDC in the coroutine context") {
        runBlocking {
          launch {
            withCoroutineTracingContext(publishedArtifact) {
              expectThat(MDC.get(X_MANAGED_DELIVERY_ARTIFACT))
                .isEqualTo(publishedArtifact.traceId)
            }
          }
        }
      }

      test("does not mix up $X_MANAGED_DELIVERY_ARTIFACT between parallel coroutines") {
        runBlocking {
          val coroutine1 = async {
            withCoroutineTracingContext(publishedArtifact) {
              println("Resource trace ID: ${MDC.get(X_MANAGED_DELIVERY_ARTIFACT)}")
              expectThat(MDC.get(X_MANAGED_DELIVERY_ARTIFACT))
                .isEqualTo(publishedArtifact.traceId)
            }
          }
          val coroutine2 = async {
            val anotherArtifact = publishedArtifact.copy(version = "anotherVersion")
            withCoroutineTracingContext(anotherArtifact) {
              println("Resource trace ID: ${MDC.get(X_MANAGED_DELIVERY_ARTIFACT)}")
              expectThat(MDC.get(X_MANAGED_DELIVERY_ARTIFACT))
                .isEqualTo(anotherArtifact.traceId)
            }
          }
          coroutine1.await()
          coroutine2.await()
        }
      }
    }
  }
}
