package com.netflix.spinnaker.keel.titus.jackson

import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.deser.std.StdNodeBasedDeserializer
import com.netflix.spinnaker.keel.clouddriver.model.PlatformSidecar
import com.netflix.spinnaker.keel.serialization.mapper
import com.fasterxml.jackson.module.kotlin.readValue

/**
 * Custom deserializer for [PlatformSidecar] field in Titus server group payloads
 *
 * We need a custom deserializer because clouddriver returns the [PlatformSidecar.arguments] field as a string-encoded
 * json object, e.g. "{\"foo\": \"bar\"}"
 * We want to represent the args within keel as a map, not a string
 */
object PlatformSidecarDeserializer : StdNodeBasedDeserializer<PlatformSidecar>(PlatformSidecar::class.java) {
  override fun convert(root: JsonNode, ctxt: DeserializationContext) =
    PlatformSidecar(
      name = root.path("name").textValue(),
      channel = root.path("channel").textValue(),
      arguments = root.path("arguments")?.textValue()?.let {
        ctxt.mapper.readValue<Map<String, Any>>(it)
      }
    )
}
