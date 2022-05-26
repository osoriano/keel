package com.netflix.spinnaker.keel.dgs

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.graphql.dgs.DgsComponent
import com.netflix.graphql.dgs.DgsDataFetchingEnvironment
import com.netflix.graphql.dgs.DgsQuery
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.graphql.types.MD_JsonSchema
import com.netflix.spinnaker.keel.schema.Generator
import com.netflix.spinnaker.keel.schema.generateSchema

@DgsComponent
class JsonSchema(
  private val generator: Generator,
  private val mapper: ObjectMapper
) {

  @DgsQuery
  suspend fun md_jsonSchema(dfe: DgsDataFetchingEnvironment): MD_JsonSchema {
    val schema = generator.generateSchema<SubmittedDeliveryConfig>()
    return MD_JsonSchema(
      id = "mdJsonSchema",
      schema = mapper.convertValue<Map<String, Any?>>(schema)
    )
  }
}
