package com.netflix.spinnaker.keel.api

/**
 * The result of a comparison between two objects. Implementations may delegate to a lower-level diff mechanism.
 */
interface ResourceDiff<T : Any> {
  val desired: T
  val current: T?
  val affectedRootPropertyTypes: List<Class<*>>
  val affectedRootPropertyNames: Set<String>
  fun hasChanges(): Boolean
  fun toDeltaJson(): Map<String, Any?>
  fun toUpdateJson(): Map<String, Any?>
  fun toDebug(): String
  fun T?.toMap(): Map<String, Any?>?
}

/**
 * Produces [ResourceDiff] instances by comparing objects.
 */
interface ResourceDiffFactory {
  fun <T : Any> compare(desired: T, current: T?): ResourceDiff<T>
}
