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
package com.netflix.spinnaker.keel.titus.exceptions

import com.netflix.spinnaker.keel.core.ResourceCurrentlyUnresolvable
import com.netflix.spinnaker.kork.exceptions.IntegrationException
import java.time.Instant

class NoDigestFound(repository: String, tag: String, registry: String) :
  ResourceCurrentlyUnresolvable("Docker image $repository:$tag not found in the $registry registry")

class RegistryNotFound(titusAccount: String) :
  IntegrationException("Unable to find Docker registry for Titus account $titusAccount")

class ImageTooOld(repository: String, tag: String, createdAt: Instant) :
  ResourceCurrentlyUnresolvable(
    "The Docker image $repository:$tag (created at $createdAt) is too old. To fix this, please publish a new image.")
