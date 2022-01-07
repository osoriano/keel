package com.netflix.spinnaker.keel.api.jira

/**
 * Holds response coming from jira, once an issue is created
 */
data class JiraIssueResponse (
  val id: String,
  val key: String,
  val self: String
  )
