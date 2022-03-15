/*
 *
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.spinnaker.keel.docker

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id.DEDUCTION
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy

@JsonTypeInfo(use = DEDUCTION)
@JsonSubTypes(
  Type(DigestProvider::class),
  Type(MultiReferenceContainerProvider::class),
  Type(ReferenceProvider::class),
)
sealed class ContainerProvider

data class ReferenceProvider(
  val reference: String
) : ContainerProvider()

// used for resources with multiple container references
data class MultiReferenceContainerProvider(val references : Set<String> = HashSet()) : ContainerProvider()

// used in titus handler as a way to represent a fully specified container
data class DigestProvider(
  val organization: String, // todo eb: should this be name = org/image instead, for consistency?
  val image: String,
  val digest: String
) : ContainerProvider() {
  fun repository() = "$organization/$image"
}
