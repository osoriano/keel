package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.keel.api.jira.JiraIssue
import com.netflix.spinnaker.keel.api.jira.JiraIssueResponse

/**
 * A bridge to call jira's endpoints without circular dependencies
 */
interface JiraBridge {

  suspend fun getTransitionsByIssue(issueId: String): Map<String, Any?>

  suspend fun createIssue(jiraIssue: JiraIssue): JiraIssueResponse
}
