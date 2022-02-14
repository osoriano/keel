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
package com.netflix.spinnaker.keel.persistence

import com.netflix.spinnaker.keel.pause.Pause
import com.netflix.spinnaker.keel.pause.PauseScope
import java.time.Clock

/**
 * A repository to track what scopes are paused, starting with application
 */
interface PausedRepository {
  val clock: Clock
    get() = Clock.systemUTC()

  fun getPause(scope: PauseScope, name: String): Pause?
  fun getPauses(scope: PauseScope, names: List<String>): List<Pause>

  fun pauseApplication(application: String, user: String, comment: String? = null)
  fun resumeApplication(application: String)
  fun applicationPaused(application: String): Boolean

  fun pauseResource(id: String, user: String, comment: String? = null)
  fun resumeResource(id: String)

  /**
   * Resumes a resource if the users calling resume is the same user that paused the cluster.
   * This is to support batch pause/resume, because we don't want to resume a cluster that was paused for
   * a different reason.
   */
  fun resumeResourceIfSameUser(id: String, user: String)
  fun resourcePaused(id: String): Boolean

  fun getPausedApplications(): List<String>
  fun getPausedResources(): List<String>
}
