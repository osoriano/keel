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
package com.netflix.spinnaker.keel.pause

import com.netflix.spinnaker.keel.actuation.EnvironmentTaskCanceler
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.events.ApplicationActuationPaused
import com.netflix.spinnaker.keel.events.ApplicationActuationResumed
import com.netflix.spinnaker.keel.events.ResourceActuationPaused
import com.netflix.spinnaker.keel.events.ResourceActuationResumed
import com.netflix.spinnaker.keel.pause.PauseScope.APPLICATION
import com.netflix.spinnaker.keel.pause.PauseScope.RESOURCE
import com.netflix.spinnaker.keel.persistence.PausedRepository
import com.netflix.spinnaker.keel.persistence.ResourceRepository
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import java.time.Clock

@Component
class ActuationPauser(
  val resourceRepository: ResourceRepository,
  val pausedRepository: PausedRepository,
  val publisher: ApplicationEventPublisher,
  val environmentTaskCanceler: EnvironmentTaskCanceler,
  val clock: Clock
) {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  fun isPaused(resource: Resource<*>): Boolean =
    getPauseScope(resource) != null

  fun isPaused(id: String): Boolean {
    val resource = resourceRepository.get(id)
    return isPaused(resource)
  }

  fun getPauseScope(resource: Resource<*>): PauseScope? =
    when {
      applicationIsPaused(resource.application) -> APPLICATION
      resourceIsPaused(resource.id) -> RESOURCE
      else -> null
    }

  fun applicationIsPaused(application: String): Boolean =
    pausedRepository.applicationPaused(application)

  fun getApplicationPauseInfo(application: String): Pause? =
    pausedRepository.getPause(APPLICATION, application)

  fun resourceIsPaused(id: String): Boolean =
    pausedRepository.resourcePaused(id)

  fun getResourcePauseInfo(id: String): Pause? =
    pausedRepository.getPause(RESOURCE, id)

  fun bulkPauseInfo(scope: PauseScope, names: List<String>): List<Pause> =
    pausedRepository.getPauses(scope, names)

  fun pauseApplication(application: String, user: String, comment: String? = null, cancelTasks: Boolean = false) {
    log.info("Pausing application $application ${logReason(comment)}")
    pausedRepository.pauseApplication(application, user, comment)
    publisher.publishEvent(ApplicationActuationPaused(application, user, comment, clock))
    if (cancelTasks) {
      log.info("Canceling tasks for $application because $user paused and requested the tasks get canceled")
      environmentTaskCanceler.cancelTasksForApplication(application, user)
    }
  }

  fun resumeApplication(application: String, user: String) {
    log.info("Resuming application $application")
    pausedRepository.resumeApplication(application)
    publisher.publishEvent(ApplicationActuationResumed(application, user, clock))
  }

  fun pauseResource(id: String, user: String, comment: String? = null) {
    log.info("Pausing resource $id ${logReason(comment)}")
    pausedRepository.pauseResource(id, user, comment)
    publisher.publishEvent(ResourceActuationPaused(resourceRepository.get(id), user, clock))
  }

  fun pauseCluster(name: String, titusAccount: String, ec2Account: String, user: String, comment: String) {
    log.info("Preparing to pause cluster $name ${logReason(comment)}")
    getClusters(name, titusAccount, ec2Account)
      .forEach { resourceId ->
        pauseResource(resourceId, user, comment)
      }
  }

  /**
   * Resumes resource if it was paused by the user calling resume, so that batch resume requests don't
   * resume resources that were paused by someone else.
   */
  fun resumeCluster(name: String, titusAccount: String, ec2Account: String, user: String, comment: String) {
    log.info("Preparing to resume cluster $name ${logReason(comment)}")
    getClusters(name, titusAccount, ec2Account)
      .forEach { resourceId ->
        resumeResourceSameUser(resourceId, user)
      }
  }

  fun getClusters(name: String, titusAccount: String, ec2Account: String): List<String> {
    // get clusters in all accounts with a matching name
    val clusters = resourceRepository.getResourceIdsForClusterName(name)
    val titusPrefix = "titus:cluster:$titusAccount"
    val ec2prefix = "ec2:cluster:$ec2Account"
    // limit to the specified accounts
    return clusters.filter { it.startsWith(titusPrefix) || it.startsWith(ec2prefix) }
  }

  fun resumeResource(id: String, user: String) {
    log.info("Resuming resource $id")
    pausedRepository.resumeResource(id)
    publisher.publishEvent(ResourceActuationResumed(resourceRepository.get(id), user, clock))
  }

  fun resumeResourceSameUser(id: String, user: String) {
    log.info("Resuming resource $id if pause user is $user")
    pausedRepository.resumeResourceIfSameUser(id, user)
    publisher.publishEvent(ResourceActuationResumed(resourceRepository.get(id), user, clock))
  }

  fun pausedApplications(): List<String> =
    pausedRepository.getPausedApplications()

  fun logReason(comment: String?) =
    comment?.let { "with comment '$it'" }
}
