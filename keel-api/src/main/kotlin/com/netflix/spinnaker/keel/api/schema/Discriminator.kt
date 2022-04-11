package com.netflix.spinnaker.keel.api.schema

import kotlin.annotation.AnnotationTarget.PROPERTY

/**
 * Marks a property as the discriminator for polymorphism.
 */
@Target(PROPERTY)
@Deprecated("can we look at @JsonTypeInfo?")
annotation class Discriminator
