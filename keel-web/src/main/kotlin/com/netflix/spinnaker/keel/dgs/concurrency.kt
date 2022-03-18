package com.netflix.spinnaker.keel.dgs

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.function.Supplier

fun <T : Any?> Executor.supplyAsync(supplier: Supplier<T>): CompletableFuture<T> =
  CompletableFuture.supplyAsync(supplier, this)
