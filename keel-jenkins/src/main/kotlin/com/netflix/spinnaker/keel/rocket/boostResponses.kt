package com.netflix.spinnaker.keel.rocket

import com.fasterxml.jackson.annotation.JsonAlias
import com.netflix.spinnaker.keel.jenkins.Job

data class JobsResponse(
  @JsonAlias("_embedded")
  val _jobs: Map<String, List<Job>>
) {
  val jobs: List<Job>
    get() = _jobs["jobs"]!!
}
