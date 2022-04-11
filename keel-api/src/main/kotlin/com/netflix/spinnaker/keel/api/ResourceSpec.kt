package com.netflix.spinnaker.keel.api

import com.fasterxml.jackson.annotation.JsonIgnore

/**
 * Implemented by all resource specs.
 */
interface ResourceSpec {

  /**
   * The standard implementation requires a `name` property in [metadata].
   */
  fun generateId(metadata: Map<String, Any?>): String = requireNotNull(metadata["name"]?.toString()) {
    "A name property is required in the metadata for a ${javaClass.simpleName}"
  }

  /**
   * Indicates whether this resource should be added to a preview environment
   **/
  @JsonIgnore
  fun isPreviewable(): Boolean = true

  /**
   * Applies the given [suffix] to the resource [id], and to all aggregate properties of the spec
   * whose names are derived from the [id].
   *
   * @return a copy of the original [ResourceSpec] with the modified identifiers.
   */
  fun deepRename(suffix: String): ResourceSpec =
    throw UnsupportedOperationException("Not implemented")
}
