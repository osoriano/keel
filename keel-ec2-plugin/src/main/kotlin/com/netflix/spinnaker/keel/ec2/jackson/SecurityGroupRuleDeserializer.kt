package com.netflix.spinnaker.keel.ec2.jackson

import com.netflix.spinnaker.keel.api.ec2.SecurityGroupRule
import com.netflix.spinnaker.keel.jackson.PropertyNamePolymorphicDeserializer

abstract class SecurityGroupRuleDeserializer :
  PropertyNamePolymorphicDeserializer<SecurityGroupRule>(SecurityGroupRule::class.java)
