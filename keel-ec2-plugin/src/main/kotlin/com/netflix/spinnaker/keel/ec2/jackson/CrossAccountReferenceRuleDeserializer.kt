package com.netflix.spinnaker.keel.ec2.jackson

import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.deser.std.StdNodeBasedDeserializer
import com.netflix.spinnaker.keel.api.SimpleLocations
import com.netflix.spinnaker.keel.api.ec2.CrossAccountReferenceRule
import com.netflix.spinnaker.keel.api.ec2.ReferenceRule
import com.netflix.spinnaker.keel.api.ec2.SecurityGroupRule

/**
 * Deserializer for [CrossAccountReferenceRule] that will actually return a [ReferenceRule] if the
 * [CrossAccountReferenceRule.account] is the same as the containing security group's account (i.e. it's not actually
 * cross-account). This lets us be lenient in allowing users to include the `account` field even if it's actually
 * redundant.
 */
class CrossAccountReferenceRuleDeserializer :
  StdNodeBasedDeserializer<SecurityGroupRule>(SecurityGroupRule::class.java) {
  override fun convert(root: JsonNode, context: DeserializationContext): SecurityGroupRule {
    val account = root.get("account").textValue()
    val locations: SimpleLocations = context.findInjectableValue("locations")
    return if (account != null && account != locations.account) {
      // can't just use context.parse otherwise we'll end up with a stack overflow
      CrossAccountReferenceRule(
        protocol = context.parse(root.get("protocol")),
        name = root.get("name").textValue(),
        account = account,
        vpc = root.get("vpc").textValue(),
        portRange = context.parse(root.get("portRange"))
      )
    } else {
      context.parse<ReferenceRule>(root)
    }
  }
}
