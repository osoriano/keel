package com.netflix.spinnaker.keel.logging

import brave.Tracer
import com.netflix.spinnaker.keel.api.Exportable
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import org.slf4j.MDC

/**
 * Support for tracing system objects in log statements via MDC in coroutines.
 */
const val X_MANAGED_DELIVERY_RESOURCE = "X-MANAGED-DELIVERY-RESOURCE"
const val X_MANAGED_DELIVERY_ARTIFACT = "X-MANAGED-DELIVERY-ARTIFACT"
val blankMDC: MDCContext = MDCContext(emptyMap())

suspend fun <T : ResourceSpec, R> withTracingContext(
  resource: Resource<T>,
  tracer: Tracer? = null,
  block: suspend CoroutineScope.() -> R
): R {
  return withTracingContext(X_MANAGED_DELIVERY_RESOURCE, resource.id, tracer , block)
}

suspend fun <R> withTracingContext(
  exportable: Exportable,
  tracer: Tracer? = null,
  block: suspend CoroutineScope.() -> R
): R {
  return withTracingContext(X_MANAGED_DELIVERY_RESOURCE, exportable.toResourceId(), tracer, block)
}

private suspend fun <R> withTracingContext(
  mdcHeader: String,
  traceId: String,
  tracer: Tracer? = null,
  block: suspend CoroutineScope.() -> R
): R {
  val parentSpan = tracer?.currentSpan()?.context()
  val span = parentSpan?.run { tracer.startScopedSpanWithParent(mdcHeader.lowercase(), parentSpan) }
    ?: tracer?.startScopedSpan(mdcHeader.lowercase())
  try {
    MDC.put(mdcHeader, traceId)
    return withContext(MDCContext(), block)
  } finally {
    MDC.remove(mdcHeader)
    span?.finish()
  }
}

fun <R> withThreadTracingContext(
  artifact: DeliveryArtifact,
  version: String,
  block: () -> R
): R = withThreadTracingContext(artifact.toArtifactVersion(version), block)

fun <R> withThreadTracingContext(
  publishedArtifact: PublishedArtifact,
  block: () -> R
): R {
  return try {
    MDC.put(X_MANAGED_DELIVERY_ARTIFACT, publishedArtifact.traceId)
    block()
  } finally {
    MDC.remove(X_MANAGED_DELIVERY_ARTIFACT)
  }
}

suspend fun <R> withCoroutineTracingContext(
  artifact: DeliveryArtifact,
  version: String,
  tracer: Tracer? = null,
  block: suspend CoroutineScope.() -> R
): R = withCoroutineTracingContext(artifact.toArtifactVersion(version), tracer, block)

suspend fun <R> withCoroutineTracingContext(
  publishedArtifact: PublishedArtifact,
  tracer: Tracer? = null,
  block: suspend CoroutineScope.() -> R
): R = withTracingContext(X_MANAGED_DELIVERY_ARTIFACT, publishedArtifact.traceId, tracer, block)

internal val PublishedArtifact.traceId: String
  get() = version.let {
    if (it.startsWith("$name-")) it.substringAfter("$name-") else it
  }.let { normalizedVersion ->
    "${type.lowercase()}:$name:$normalizedVersion"
  }
