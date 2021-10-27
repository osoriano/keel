package com.netflix.spinnaker.keel.jackson.mixins

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.netflix.spinnaker.keel.api.ArtifactChange
import com.netflix.spinnaker.keel.api.ResourceChange
import com.netflix.spinnaker.keel.api.UnknownChange

@JsonTypeInfo(
  use = Id.NAME,
  include = As.EXISTING_PROPERTY,
  property = "reason"
)
@JsonSubTypes(
  Type(value = ArtifactChange::class, name = "artifact"),
  Type(value = ResourceChange::class, name = "resource"),
  Type(value = UnknownChange::class, name = "unknown")
)
interface EnvironmentChangeReasonMixin
