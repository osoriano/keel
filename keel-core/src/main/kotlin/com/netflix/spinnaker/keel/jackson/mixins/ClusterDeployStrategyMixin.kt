package com.netflix.spinnaker.keel.jackson.mixins

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.netflix.spinnaker.keel.api.Highlander
import com.netflix.spinnaker.keel.api.RedBlack

@JsonTypeInfo(
  use = Id.NAME,
  include = As.EXISTING_PROPERTY,
  property = "strategy"
)
@JsonSubTypes(
  Type(value = RedBlack::class, name = "red-black"),
  Type(value = Highlander::class, name = "highlander")
)
interface ClusterDeployStrategyMixin
