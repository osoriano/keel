package com.netflix.spinnaker.keel.api.artifacts

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.netflix.spinnaker.keel.api.schema.Discriminator

/**
 * Strategy for how to sort versions of artifacts.
 */
@JsonTypeInfo(
  use = Id.NAME,
  include = As.EXISTING_PROPERTY,
  property = "type"
)
interface SortingStrategy {
  @Discriminator
  val type: String
  val comparator: Comparator<PublishedArtifact>
}
