package com.netflix.spinnaker.keel.jackson.mixins

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.netflix.spinnaker.keel.api.Alphabetical
import com.netflix.spinnaker.keel.api.OffStreamingPeak
import com.netflix.spinnaker.keel.api.Staggered

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  property = "type",
  include = JsonTypeInfo.As.PROPERTY
)
@JsonSubTypes(
  JsonSubTypes.Type(value = Alphabetical::class),
  JsonSubTypes.Type(value = OffStreamingPeak::class),
  JsonSubTypes.Type(value = Staggered::class)
)
interface RolloutStrategyMixin
