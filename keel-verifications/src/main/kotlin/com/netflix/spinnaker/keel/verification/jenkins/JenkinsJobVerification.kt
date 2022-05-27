package com.netflix.spinnaker.keel.verification.jenkins

import com.fasterxml.jackson.annotation.JsonIgnore
import com.netflix.spinnaker.keel.api.Verification
import com.netflix.spinnaker.keel.api.schema.Title
import com.netflix.spinnaker.keel.verification.StandardTestParameter
import org.apache.commons.codec.digest.DigestUtils

/**
 * A [Verification] that uses a Jenkins job to check the state of the environment.
 */
data class JenkinsJobVerification(
  val controller: String,
  val job: String,
  @Title("Static parameters")
  val staticParameters: Map<String, Any> = emptyMap(),
  @Title("Dynamic parameters")
  val dynamicParameters: Map<String, StandardTestParameter> = emptyMap()
) : Verification {
  companion object {
    const val TYPE = "jenkins-job"
  }

  override val type: String = TYPE

  @get:JsonIgnore
  override val id by lazy {
    "$controller:$job#${DigestUtils.sha1Hex(staticParameters.toString() + dynamicParameters.toString())}"
  }

  @get:JsonIgnore
  val name: String = "$controller/$job"
}
