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

import com.netflix.spinnaker.keel.pause.PauseScope
import com.netflix.spinnaker.keel.pause.PauseScope.APPLICATION
import com.netflix.spinnaker.keel.pause.PauseScope.RESOURCE
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isFalse
import strikt.assertions.isTrue

abstract class PausedRepositoryTests<T : PausedRepository> : JUnit5Minutests {
  abstract fun factory(): T

  open fun T.flush() {}

  val application = "keeldemo"
  val resource = "keel-cluster"

  data class Fixture<T : PausedRepository>(
    val subject: T
  )

  fun tests() = rootContext<Fixture<T>> {
    fixture {
      Fixture(subject = factory())
    }

    after { subject.flush() }

    context("application not vetoed") {
      test("shows not paused") {
        expectThat(subject.applicationPaused(application)).isFalse()
      }

      test("no applications are paused") {
        expectThat(subject.getPausedApplications()).isEmpty()
      }
    }

    context("application paused") {
      before {
        subject.pauseApplication(application, "keel@keel.io")
      }

      test("app appears in list of paused apps") {
        expectThat(subject.getPausedApplications()).containsExactlyInAnyOrder(application)
      }

      test("paused reflects correctly") {
        expectThat(subject.applicationPaused(application)).isTrue()
      }

      test("resume works") {
        subject.resumeApplication(application)
        expectThat(subject.applicationPaused(application)).isFalse()
      }
    }

    context("several applications paused") {
      before {
        subject.pauseApplication("app1", "keel@keel.io")
        subject.pauseApplication("app2", "keel@keel.io")
        subject.pauseApplication("app3", "keel@keel.io")
      }

      test("app appears in list of paused apps") {
        val pauses = subject.getPauses(APPLICATION, listOf("app1", "app2", "app3", "app4", "app5"))
        expect {
          that(pauses).hasSize(3)
          that(pauses.map { it.name }).containsExactlyInAnyOrder("app1", "app2", "app3")
        }
      }
    }

    context("resource paused") {
      before {
        subject.pauseResource(resource, "keel@keel.io")
      }

      test("resume works") {
        subject.resumeResource(resource)
        expectThat(subject.resourcePaused(resource)).isFalse()
      }

      test("resume doesn't work with different user") {
        subject.resumeResourceIfSameUser(resource, "me@hi.org")
        expectThat(subject.resourcePaused(resource)).isTrue()
      }
    }

    context("several resources paused") {
      before {
        subject.pauseResource("resource1", "keel@keel.io")
        subject.pauseResource("resource2", "keel@keel.io")
        subject.pauseResource("resource3", "keel@keel.io")
      }

      test("resources appears in list of paused resources") {
        val pauses = subject.getPauses(RESOURCE, listOf("resource1", "resource2", "resource3", "resource4", "resource5"))
        expect {
          that(pauses).hasSize(3)
          that(pauses.map { it.name }).containsExactlyInAnyOrder("resource1", "resource2", "resource3")
        }
      }
    }
  }
}
