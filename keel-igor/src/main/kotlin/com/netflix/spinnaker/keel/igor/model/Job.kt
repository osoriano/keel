package com.netflix.spinnaker.keel.igor.model

import com.fasterxml.jackson.annotation.JsonInclude

/**
 * An immutable data class that represents a CI job.
 *
 * This class mirrors [com.netflix.spinnaker.igor.build.model.GenericJob].
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
data class Job(
  val name: String,
  val cron: Boolean? = false,
  val repo: Repo? = null,
  val scmType: String,
  val branchPattern: String? = null,
  val createdAt: String,
  val updatedAt: String
)

class Repo(
  val hostType: String,
  val projectKey: String,
  val repoSlug: String
)
