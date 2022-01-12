package com.netflix.spinnaker.keel.schema

import com.netflix.spinnaker.keel.api.NamedResource
import kotlin.reflect.KClass

class NamedResourceSchemaCustomizer(
  private val resourceKindSchemaCustomizer: ResourceKindSchemaCustomizer
) : SchemaCustomizer {
  companion object {
    val NAMED_RESOURCE_TYPE = NamedResource::class
  }

  override fun supports(type: KClass<*>): Boolean =
    type == NAMED_RESOURCE_TYPE

  override fun buildSchema(): Schema {
    val type = NAMED_RESOURCE_TYPE
    return ObjectSchema(
      title = checkNotNull(type.simpleName),
      description = null,
      properties = mapOf(
        "name" to StringSchema("The name of a resource, or * to match all resources of the specified kind."),
        "kind" to resourceKindSchemaCustomizer.buildSchema()
      ),
      additionalProperties = false,
      required = sortedSetOf("name", "kind")
    )
  }
}
