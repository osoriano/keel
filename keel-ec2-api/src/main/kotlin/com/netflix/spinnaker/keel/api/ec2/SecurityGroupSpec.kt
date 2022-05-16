/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.keel.api.ec2

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY
import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.SimpleLocations
import com.netflix.spinnaker.keel.api.SpinnakerResourceSpec
import com.netflix.spinnaker.keel.api.schema.Optional
import com.netflix.spinnaker.keel.api.schema.Title

@Title("Security group")
data class SecurityGroupSpec(
  override val moniker: Moniker,
  @Optional override val locations: SimpleLocations,
  val description: String?,
  val inboundRules: Set<SecurityGroupRule> = emptySet(),
  @get:JsonInclude(NON_EMPTY)
  val overrides: Map<String, SecurityGroupOverride> = emptyMap()
) : SpinnakerResourceSpec<SimpleLocations> {
  companion object {
    const val MAX_NAME_LENGTH = 255
  }

  override fun deepRename(suffix: String): SecurityGroupSpec {
    return copy(
      moniker = moniker.withSuffix(suffix, canTruncateStack = false, maxNameLength = MAX_NAME_LENGTH),
      inboundRules = inboundRules.map { rule ->
        if (rule is ReferenceRule && rule.name == moniker.toName()) {
          rule.copy(name = moniker.withSuffix(suffix, canTruncateStack = false, maxNameLength = MAX_NAME_LENGTH).toName())
        } else {
          rule
        }
      }.toSet()
    )
  }
}

data class SecurityGroupOverride(
  val description: String? = null,
  val inboundRules: Set<SecurityGroupRule>? = null,
  val vpc: String? = null
)
