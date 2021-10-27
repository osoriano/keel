package com.netflix.spinnaker.keel.igor

data class RawDeliveryConfigResult(
  val manifest: String
)


data class GraphqlSchemaFile(
  val path: String,
  val contents: String
)

data class GraphqlSchemaResult(
  val schemas: List<GraphqlSchemaFile>,
) {
  val schema : String
    get() = schemas.joinToString("\n") { it.contents }
}
