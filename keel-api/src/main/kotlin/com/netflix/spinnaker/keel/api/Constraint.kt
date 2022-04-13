package com.netflix.spinnaker.keel.api

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.netflix.spinnaker.keel.api.schema.Discriminator

@JsonTypeInfo(
  use = Id.NAME,
  include = As.EXISTING_PROPERTY,
  property = "type"
)
abstract class Constraint(@Discriminator val type: String)
abstract class StatefulConstraint(type: String) : Constraint(type)
abstract class DeploymentConstraint(type: String) : Constraint(type)
