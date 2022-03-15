/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.keel.api.constraints

import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.UID
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact

interface ConstraintRepository {

  /**
   * @return the current state of the constraint, creating a new pending state if none exists
   */
  fun getCurrentState(
    artifact: DeliveryArtifact,
    version: String,
    deliveryConfig: DeliveryConfig,
    targetEnvironment: Environment,
    constraint: Constraint,
  ): ConstraintState {
    var state = getConstraintState(
      deliveryConfig.name,
      targetEnvironment.name,
      version,
      constraint.type,
      artifact.reference
    )

    if (state == null) {
      state = ConstraintState(
        deliveryConfigName = deliveryConfig.name,
        environmentName = targetEnvironment.name,
        artifactVersion = version,
        artifactReference = artifact.reference,
        type = constraint.type,
        status = ConstraintStatus.PENDING
      )
      storeConstraintState(state)
    }

    return state
  }

  fun storeConstraintState(state: ConstraintState)

  fun getConstraintState(deliveryConfigName: String, environmentName: String, artifactVersion: String, type: String, artifactReference: String?): ConstraintState?

  fun getConstraintStateById(uid: UID): ConstraintState?

  fun deleteConstraintState(deliveryConfigName: String, environmentName: String, reference: String, version: String, type: String): Int

  fun constraintStateFor(application: String): List<ConstraintState>

  fun constraintStateFor(deliveryConfigName: String, environmentName: String, limit: Int): List<ConstraintState>

  fun constraintStateFor(deliveryConfigName: String, environmentName: String, artifactVersion: String, artifactReference: String): List<ConstraintState>
}
