package com.netflix.spinnaker.keel.schema

import kotlin.reflect.KClass

interface SchemaCustomizer {
  fun supports(type: KClass<*>): Boolean

  fun buildSchema(): Schema
}
