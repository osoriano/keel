package com.netflix.spinnaker.keel.diff

import de.danielbechler.diff.comparison.ComparisonConfigurer
import de.danielbechler.diff.comparison.ComparisonConfigurer.Of
import de.danielbechler.diff.identity.IdentityConfigurer.OfCollectionItems
import de.danielbechler.diff.identity.IdentityService
import de.danielbechler.diff.node.DiffNode
import kotlin.reflect.KProperty1

inline fun <reified T : Any> ComparisonConfigurer.ofType(): Of = ofType(T::class.java)

inline fun <reified T : Annotation> DiffNode.getPropertyAnnotation(): T? =
  getPropertyAnnotation(T::class.java)

inline fun <reified T : Any, V> IdentityService.ofCollectionItems(property: KProperty1<T, V>): OfCollectionItems =
  ofCollectionItems(T::class.java, property.name)
