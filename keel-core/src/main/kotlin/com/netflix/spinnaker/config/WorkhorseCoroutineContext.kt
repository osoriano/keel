package com.netflix.spinnaker.config

import kotlinx.coroutines.CoroutineDispatcher
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers

/**
 * A thin wrapper around [CoroutineContext] to signal the purpose of this context where it's injected, which
 * is to do costly work that generally involves I/O.
 *
 * Declare a dependency of this type in your beans if it does that kind of work. A common pattern is to
 * implement [CoroutineScope] like so:
 *
 * ```kotlin
 *  @Component
 *  class MyComponent(
 *    override val coroutineContext: WorkhorseCoroutineContext
 *  ) : CoroutineScope
 * ```
 */
class WorkhorseCoroutineContext(private val delegate: CoroutineDispatcher) : CoroutineContext by delegate

val DefaultWorkhorseCoroutineContext = WorkhorseCoroutineContext(Dispatchers.IO)
