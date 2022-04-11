package com.netflix.spinnaker.keel.api.ec2

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.netflix.spinnaker.keel.api.schema.Discriminator

/**
 * Extensible mechanism allowing for abstraction of details of the VM instance (e.g.
 * [LaunchConfigurationSpec.instanceType], [LaunchConfigurationSpec.ebsOptimized], etc.)
 */
@JsonTypeInfo(
  use = Id.NAME,
  include = As.EXISTING_PROPERTY,
  property = "type"
)
interface InstanceProvider {
  @Discriminator
  val type: String
}
