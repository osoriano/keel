package com.netflix.spinnaker.keel.ec2.jackson.mixins

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.netflix.spinnaker.keel.ec2.jackson.CrossAccountReferenceRuleDeserializer

@JsonDeserialize(using = CrossAccountReferenceRuleDeserializer::class)
interface CrossAccountReferenceRuleMixin
