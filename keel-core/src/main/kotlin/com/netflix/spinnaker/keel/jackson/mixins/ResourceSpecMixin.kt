package com.netflix.spinnaker.keel.jackson.mixins

import com.fasterxml.jackson.annotation.JsonIgnore

interface ResourceSpecMixin {
  @get:JsonIgnore
  val displayName: String

  /**
   * Indicates whether this resource should be added to a preview environment
   *
   * Annotated with [JsonIgnore] because the return value is always derived from other
   * properties of a spec, and so there's no need to encode it in the serialization.
   **/
  @JsonIgnore
  fun isPreviewable(): Boolean = true

}
