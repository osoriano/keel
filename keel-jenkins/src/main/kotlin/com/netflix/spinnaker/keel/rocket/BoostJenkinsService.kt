package com.netflix.spinnaker.keel.rocket

import com.netflix.spinnaker.keel.jenkins.JenkinsService
import com.netflix.spinnaker.keel.jenkins.JobConfig
import com.netflix.spinnaker.keel.jenkins.JobState.ACTIVE
import com.netflix.spinnaker.keel.jenkins.ScmType
import com.netflix.spinnaker.kork.web.exceptions.NotFoundException
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Rocket Boost implementation of [JenkinsService]. Netflix-specific.
 */
@Component
class BoostJenkinsService(
  private val boostApi: BoostApi
): JenkinsService {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  override suspend fun getJobConfig(controller: String, job: String): JobConfig {
    val response = boostApi.getJob(controller, job)
    val jobConfig = response.jobs.firstOrNull()?.config
      ?: throw NotFoundException("Jenkins job $job at controller $controller not found.")

    log.debug("Retrieved Jenkins job config: $jobConfig")
    return jobConfig
  }

  override suspend fun hasRocketJob(job: String): Boolean {
    val response = boostApi.getJob(job = job, state = ACTIVE)
    return response.jobs.any {
      it.scmType == ScmType.ROCKET
    }
  }

}
