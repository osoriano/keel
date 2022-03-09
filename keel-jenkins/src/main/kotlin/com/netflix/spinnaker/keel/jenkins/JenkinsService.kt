package com.netflix.spinnaker.keel.jenkins

/**
 * A high-level abstraction of the Jenkins REST API.
 */
interface JenkinsService {
  suspend fun getJobConfig(controller: String, job: String): JobConfig
  suspend fun hasRocketJob(jobName: String): Boolean
}
