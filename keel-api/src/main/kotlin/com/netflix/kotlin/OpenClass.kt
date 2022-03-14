package com.netflix.kotlin

/**
 * Marker annotation to use with the kotlin-allopen compiler plugin. Open classes are required
 * for Spring auto-proxying, which in turn is required by tracing instrumentation.
 */
annotation class OpenClass