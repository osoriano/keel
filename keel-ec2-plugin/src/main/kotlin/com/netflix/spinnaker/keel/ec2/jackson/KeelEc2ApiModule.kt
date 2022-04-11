package com.netflix.spinnaker.keel.ec2.jackson

import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.module.SimpleModule
import com.netflix.spinnaker.keel.api.ec2.CidrRule
import com.netflix.spinnaker.keel.api.ec2.CrossAccountReferenceRule
import com.netflix.spinnaker.keel.api.ec2.EC2ScalingSpec
<<<<<<< 8bc4d5c98f5c3f32ae6f652ee476ddf0a52dd4bb
import com.netflix.spinnaker.keel.api.ec2.InstanceProvider
import com.netflix.spinnaker.keel.api.ec2.PrefixListRule
=======
>>>>>>> 1eda9481718f75eb72e089f57853541f2a392f33
import com.netflix.spinnaker.keel.api.ec2.ReferenceRule
import com.netflix.spinnaker.keel.api.ec2.ScalingSpec
import com.netflix.spinnaker.keel.api.ec2.SecurityGroupRule
import com.netflix.spinnaker.keel.api.support.ExtensionRegistry
import com.netflix.spinnaker.keel.ec2.jackson.mixins.CrossAccountReferenceRuleMixin

val SECURITY_GROUP_RULE_SUBTYPES = mapOf(
  ReferenceRule::class.java to "reference",
  CrossAccountReferenceRule::class.java to "cross-account",
  CidrRule::class.java to "cidr",
  PrefixListRule::class.java to "prefix-list"
)

fun ObjectMapper.registerKeelEc2ApiModule(): ObjectMapper =
  registerModule(KeelEc2ApiModule)
    .apply {
      SECURITY_GROUP_RULE_SUBTYPES.forEach { (subType, discriminator) ->
        registerSubtypes(NamedType(subType, discriminator))
      }
      registerSubtypes(EC2ScalingSpec::class.java)
    }

fun ExtensionRegistry.registerEc2Subtypes() {
  // Note that the discriminators below are not used as sub-types are determined by the custom deserializer above
  SECURITY_GROUP_RULE_SUBTYPES.forEach { (subType, discriminator) ->
    register(SecurityGroupRule::class.java, subType, discriminator)
  }
  register(ScalingSpec::class.java, EC2ScalingSpec::class.java, "ec2")
}

internal object KeelEc2ApiModule : SimpleModule("Keel EC2 API") {
  override fun setupModule(context: SetupContext) {
    with(context) {
      setMixInAnnotations<CrossAccountReferenceRule, CrossAccountReferenceRuleMixin>()
    }
    super.setupModule(context)
  }
}

private inline fun <reified TARGET, reified MIXIN> Module.SetupContext.setMixInAnnotations() {
  setMixInAnnotations(TARGET::class.java, MIXIN::class.java)
}
